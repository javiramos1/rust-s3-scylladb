mod config;
mod data;
mod db;
mod s3;

extern crate serde_json;
extern crate num_cpus;

use crate::config::Config;
use crate::data::model::get_id_from_url;
use crate::data::rest_api::TraversalNodeRequest;
use crate::db::scylladb::ScyllaDbService;
use crate::s3::s3::read_file;
use actix_web::error::ErrorInternalServerError;
use actix_web::middleware::Logger;
use actix_web::web::Json;
use actix_web::{get, post, web, web::Data, App, Error, HttpResponse, HttpServer};
use color_eyre::Result;
use data::model::{Node, Relation, TraversalNode};
use data::rest_api::{GetNodeRequest, IngestionRequest};
use data::source_model::{Relation as SourceRelation, Nodes};
use db::model::DbNode;
use futures::future::{BoxFuture, FutureExt};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};
use tokio::task;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use uuid::Uuid;

use std::string::ToString;
use strum_macros::Display;

#[derive(Display, Debug)]enum DIR {
    IN,
    OUT
}

#[derive(Display, Debug)]
enum REL {
    ISPARENT,
    ISCHILD
}

struct AppState {
    db_svc: ScyllaDbService,
    semaphore: Arc<Semaphore>,
    region: String
}

#[get("/node/{id}")]
async fn get_by_id(
    path: web::Path<String>,
    query_data: web::Query<GetNodeRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let now = Instant::now();
    let id = path.into_inner();
    info!("get_by_id {}, relations? {:?}", id, query_data);

    let relations = query_data.get_relations.unwrap_or_default();
    let tags = query_data.get_tags.unwrap_or(true);

    let ret = get_node(&state.db_svc, &id, tags, relations).await?;

    let elapsed = now.elapsed();
    info!("get_by_id time: {:.2?}", elapsed);
    Ok(HttpResponse::Ok().json(ret))
}

#[get("/traversal/{id}")]
async fn traversal_by_id(
    path: web::Path<String>,
    query_data: web::Query<TraversalNodeRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    let now = Instant::now();
    let id = path.into_inner();
    info!("traversal_by_id: {}", id);

    let result = traversal_recur(
        state,
        id,
        Arc::new(query_data.direction.clone()),
        Arc::new(query_data.relation_type.clone()),
        0,
        query_data.max_depth
    )
    .await;

    let elapsed = now.elapsed();
    info!("traversal time: {:.2?}", elapsed);
    Ok(HttpResponse::Ok().json(result))
}

fn traversal_recur<'a>(
    state: Data<AppState>,
    id: String,
    direction: Arc<String>,
    relation_type: Arc<Option<String>>,
    depth: usize,
    max: usize,
) -> BoxFuture<'a, Option<TraversalNode>> {
    async move {
        
        let db_nodes = state
            .db_svc
            .get_node_traversal(&id, &direction, &relation_type)
            .await
            .ok()?;
        let mut node = TraversalNode::from(db_nodes, depth)?;

        if depth < max && node.relation_ids.len() > 0 {
            let mut handlers: Vec<JoinHandle<_>> = Vec::new();
           
            for id in &node.relation_ids {

                handlers.push(tokio::spawn(traversal_recur(
                    state.clone(),
                    id.to_string(),
                    direction.clone(),
                    relation_type.clone(),
                    depth + 1,
                    max,
                )));
            }
      
            for thread in handlers {
                let child = thread.await.ok()?;
                node.relations.push(child?);
            }
        }
   
        Some(node)
    }
    .boxed()
   
}

async fn get_node(
    db: &ScyllaDbService,
    id: &str,
    tags: bool,
    relations: bool,
) -> Result<Json<Option<Node>>, Error> {
    let db_nodes = db
        .get_node(id, tags, relations)
        .await
        .map_err(ErrorInternalServerError)?;

    let node = Node::from(db_nodes);

    Ok(web::Json(node))
}

#[post("/ingest")]
async fn ingest(
    payload: web::Json<IngestionRequest>,
    state: Data<AppState>,
) -> Result<HttpResponse, Error> {
    info!("Ingest Request: {:?}", payload.files);
    let now = Instant::now();

    let mut handlers: Vec<JoinHandle<_>> = Vec::new();

    for file in payload.files.iter() {
        let permit = state.semaphore.clone().acquire_owned().await;
        handlers.push(task::spawn(process_file(
            payload.ingestion_id.clone(),
            state.clone(),
            file.to_string(),
            permit,
        )));
    }

    debug!("Waiting for files to be processed...");
    for thread in handlers {
        match thread.await {
            Err(e) => return Err(ErrorInternalServerError(e)),
            Ok(r) => {
                if let Err(e) = r {
                    error!("Error: {:?}", e);
                    return Err(ErrorInternalServerError(e));
                }
            }
        }
    }

    let elapsed = now.elapsed();
    info!("Ingestion Time: {:.2?}", elapsed);
    Ok(HttpResponse::Ok().json(r#"{ "status": "OK"}"#))
}

async fn process_file(
    ingestion_id: String,
    state: Data<AppState>,
    file: String,
    permit: Result<OwnedSemaphorePermit, AcquireError>,
) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
    info!(
        "Processing File {} for provider {}. Reading file...",
        file, ingestion_id
    );
    let now = Instant::now();
    let contents = read_file(&state.region, file.to_string()).await?;

    info!("File Read. Processing Relations..");
    let relations = process_relations(&ingestion_id, contents.relations);
    info!(
        "Relations procesed, size: {}. Persisting Nodes..",
        relations.len()
    );

    // Get all the nodes from the file in a flat structure and parent relations
    let nodes = process_nodes(&ingestion_id, contents.nodes, relations).await?;

    info!(
        "Nodes proccesed, nodes size: {}.Persisting...",
        nodes.len()
    );

    // persist in DB and wait
    state.db_svc.save_nodes(nodes).await?;

    info!("Nodes Persisted!");
    let elapsed = now.elapsed();
    info!("File {} processed. Took {:.2?}", file, elapsed);

    let _permit = permit;

    Ok(())
}


fn process_relations(
    ingestion_id: &str,
    relations: Vec<SourceRelation>,
) -> HashMap<String, Vec<Relation>> {
    let now = Instant::now();
    let ret = relations.iter().fold(HashMap::new(), move |mut acc, r| {
        let source = get_path(&r.source);
        let target = get_path(&r.target);

        let rel = Relation::new(
            ingestion_id.to_owned(),
            r.type_field.clone(),
            target.clone(),
            true,
        );
        acc.entry(source.clone()).or_insert_with(Vec::new).push(rel);
        // the other side
        let rel_target = Relation::new(ingestion_id.to_owned(), r.type_field.clone(), source, false);
        acc.entry(target).or_insert_with(Vec::new).push(rel_target);
        acc
    });
    let elapsed = now.elapsed();
    info!("process_relations Took {:.2?}", elapsed);
    ret
}

fn get_path(path: &[String]) -> String {
    path.iter()
        .map(|r| r.clone())
        .reduce(|a, b| format!("{}/{}", &a, &b))
        .unwrap()
}

async fn process_nodes(
    ingestion_id: &String,
    nodes: Vec<Nodes>,
    relations: HashMap<String, Vec<Relation>>,
) -> Result<Vec<DbNode>> {
    info!("process_nodes: {}", ingestion_id);
    let now = Instant::now();
    let mut db_nodes: Vec<DbNode> = Vec::new();
    let parent = None;
    let path: String = String::new();
    flatten_nodes(
        ingestion_id,
        &nodes,
        &path,
        &parent,
        &mut db_nodes,
        &relations,
    );
    let elapsed = now.elapsed();
    info!("process_nodes Took {:.2?}", elapsed);
    Ok(db_nodes)
}

fn flatten_nodes(
    ingestion_id: &String,
    nodes: &Vec<Nodes>,
    path: &String,
    parent: &Option<(Uuid, String)>,
    db_nodes: &mut Vec<DbNode>,
    relations: &HashMap<String, Vec<Relation>>,
) {
    debug!(
        "Flattening Nodes, path {}, node size {}",
        path,
        db_nodes.len()
    );
    for node in nodes {
        let empty = &mut Vec::new();
        let tags = node.tags.as_ref().get_or_insert(empty).clone();

        let mut parent_url = path.to_owned();
        let url = path.clone() + node.name.as_str();

        parent_url.pop();

        let root = DbNode::root(
            ingestion_id.clone(),
            url.clone(),
            node.name.clone(),
            node.type_field.clone(),
            tags.to_vec(),
        );

        let id = root.uuid;
        let name = root.name.clone();
     
        db_nodes.push(root);

        if parent.is_some() {
            let (parent_id, parent_name) = parent.as_ref().unwrap();

            let rel = DbNode::relation(
                id,
                ingestion_id.clone(),
                DIR::IN.to_string(),
                REL::ISPARENT.to_string(),
                parent_id.to_string(),
                parent_name.to_owned(),
            );
            db_nodes.push(rel);
        }

        let empty_rel = &mut Vec::new();
        for r in relations.get(&url).get_or_insert(empty_rel).iter() {
            db_nodes.push(DbNode::from_rel(id, ingestion_id.clone(), r));
        }

        for c in &node.children {
            let child_url = url.clone() + "/" + c.name.as_str();
            let child_id = get_id_from_url(ingestion_id.clone(), child_url);
            let rel = DbNode::relation(
                id,
                ingestion_id.clone(),
                DIR::OUT.to_string(),
                REL::ISCHILD.to_string(),
                child_id.to_string(),
                c.name.clone(),
            );
            db_nodes.push(rel);
        }

        if !node.children.is_empty() {
            let mut new_path = path.clone();
            new_path.push_str(node.name.as_str());
            new_path.push('/');
            let parent = Some((id, name));
            flatten_nodes(
                ingestion_id,
                &node.children,
                &new_path,
                &parent,
                db_nodes,
                relations,
            )
        }
    }
}

#[actix_web::main]
async fn main() -> Result<()> {
    let config = Config::from_env().expect("Server configuration");

    let port = config.port;
    let host = config.host.clone();
    let num_cpus = num_cpus::get();
    let parallel_files = config.parallel_files;
    let db_parallelism = config.db_parallelism;
    let region = config.region;

    info!(
        "Starting application. Num CPUs {}. Max Parallel Files {}. DB Parallelism {}.  Region {}",
        num_cpus, parallel_files, db_parallelism, region
    );

    let db = ScyllaDbService::new(config.db_dc, config.db_url, 
        db_parallelism, config.schema_file).await;

    let sem = Arc::new(Semaphore::new(parallel_files));
    let data = Data::new(AppState {
        db_svc: db,
        semaphore: sem,
        region
    });

    info!("Starting server at http://{}:{}/", host, port);
    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .app_data(data.clone())
            .service(ingest)
            .service(get_by_id)
            .service(traversal_by_id)
    })
    .bind(format!("{}:{}", host, port))?
    .workers(num_cpus * 2)
    .run()
    .await?;

    Ok(())
}
