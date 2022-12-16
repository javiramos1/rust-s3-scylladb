use crate::db::model::{DbNode, DbNodeSimple, DbRelation};

use scylla::prepared_statement::PreparedStatement;
use scylla::statement::Consistency;
use scylla::transport::load_balancing::{DcAwareRoundRobinPolicy, TokenAwarePolicy};
use scylla::transport::Compression;
use scylla::{Session, SessionBuilder};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use tracing::{debug, error, info};
use uuid::Uuid;
use std::time::Duration;
use std::fs;


pub struct ScyllaDbService {
    db_session: Arc<Session>,
    parallelism: usize,
    ps: Arc<PreparedStatement>,
    ps_traversal: Arc<PreparedStatement>,
    ps_traversal_relation: Arc<PreparedStatement>,
}

const INSERT_QUERY: &str = "INSERT INTO graph.nodes (id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
const GET_ONE_QUERY: &str =
    "SELECT id, name, item_type, url, ingestion_id FROM graph.nodes WHERE id = ? and direction = '' and relation = ''";
const GET_ONE_QUERY_TAGS: &str = "SELECT id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags FROM graph.nodes WHERE id = ? and direction = '' and relation = ''";
const GET_ONE_QUERY_RELATIONS: &str = "SELECT id, direction, relation, relates_to, name, ingestion_id, url, item_type, tags FROM graph.nodes WHERE id = ?";
const GET_ONE_QUERY_DIRECTION: &str = "SELECT id, direction, relation, relates_to, name, item_type FROM graph.nodes WHERE id = ? and direction in ('',?)";
const GET_ONE_QUERY_DIRECTION_RELATION: &str = "SELECT id, direction, relation, relates_to, name, item_type FROM graph.nodes WHERE id = ? and direction in ('',?) and relation in ('',?)";

impl ScyllaDbService {
    pub async fn new(dc: String, host: String, db_parallelism: usize, schema_file: String) -> Self {
        debug!("ScyllaDbService: Connecting to {}. DC: {}.", host, dc);

        let dc_robin = Box::new(DcAwareRoundRobinPolicy::new(dc.to_string()));
        let policy = Arc::new(TokenAwarePolicy::new(dc_robin));

        let session: Session = SessionBuilder::new()
            .known_node(host.clone())
            .load_balancing(policy)
            .compression(Some(Compression::Lz4))
            .build()
            .await
            .expect("Error Connecting to ScyllaDB");

        info!("ScyllaDbService: Connected to {}", host);

        info!("ScyllaDbService: Creating Schema..");
      
        let schema = fs::read_to_string(&schema_file)
        .expect(("Error Reading Schema File".to_owned() + schema_file.as_str()).as_str());

        let schema_query = schema.trim().replace("\n", "");

        for q in schema_query.split(";") {
            let query = q.to_owned() + ";";
            if query.len() > 1 {
                info!("Running Query {}", query);
                session.query(query, &[]).await.expect("Error creating schema!");
            }
          
        }

        if session
            .await_timed_schema_agreement(Duration::from_secs(10))
            .await.expect("Error Awaiting Schema Creation")
        {
            info!("Schema Created!");
        } else {
            panic!("Timed schema is NOT in agreement");
        }

        info!("ScyllaDbService: Schema created. Creating preprared query...");
        let mut ps = session
            .prepare(INSERT_QUERY)
            .await
            .expect("Error Creating Prepared Query");
        ps.set_consistency(Consistency::Any);
        let mut ps_t = session
            .prepare(GET_ONE_QUERY_DIRECTION)
            .await
            .expect("Error Creating Prepared Query");
        ps_t.set_consistency(Consistency::One);
        let mut ps_tr = session
            .prepare(GET_ONE_QUERY_DIRECTION_RELATION)
            .await
            .expect("Error Creating Prepared Query");
        ps_tr.set_consistency(Consistency::One);
        
        let db_session = Arc::new(session);
        info!("ScyllaDbService: Parallelism {}", db_parallelism);

        let prepared_query = Arc::new(ps);    
        let ps_traversal = Arc::new(ps_t);    
        let ps_traversal_relation = Arc::new(ps_tr);    

        ScyllaDbService {
            db_session,
            parallelism: db_parallelism,
            ps: prepared_query,
            ps_traversal,
            ps_traversal_relation,
        }
    }

    pub async fn get_node(
        &self,
        id: &str,
        tags: bool,
        relations: bool,
    ) -> Result<Vec<DbNode>, Box<dyn std::error::Error + Sync + Send>> {
        return self.get_node_int(id, tags, relations).await;
    }

    pub async fn get_node_traversal(
        &self,
        id: &str,
        direction: &str,
        relation_type: &Option<String>,
    ) -> Result<Vec<DbRelation>, Box<dyn std::error::Error + Sync + Send>> {
        let uuid = Uuid::parse_str(id)?;
        let mut ret = vec![];

        let session = self.db_session.clone();
        let result = match relation_type {
            Some(relation) => {
                session
                    .execute(
                        &self.ps_traversal_relation.clone(),
                        (uuid, direction, relation),
                    )
                    .await?
            }
            None => {
                session
                    .execute(
                        &self.ps_traversal.clone(),
                        (uuid, direction),
                    )
                    .await?
            }
        };

        if let Some(rows) = result.rows {
            for r in rows {
                let node = r.into_typed::<DbRelation>()?;
                ret.push(node);
            }
        }

        Ok(ret)
    }

    async fn get_node_int(
        &self,
        id: &str,
        tags: bool,
        relations: bool,
    ) -> Result<Vec<DbNode>, Box<dyn std::error::Error + Sync + Send>> {
        let now = Instant::now();
        info!("ScyllaDbService: get_node: {} relations? {}", id, relations);

        let uuid = Uuid::parse_str(id)?;
        let mut ret = vec![];

        let q = if relations {
            GET_ONE_QUERY_RELATIONS
        } else if tags {
            GET_ONE_QUERY_TAGS
        } else {
            GET_ONE_QUERY
        };

        let session = self.db_session.clone();
        let result = session.query(q, (uuid,)).await?;

        if let Some(rows) = result.rows {
            for r in rows {
                let node;
                if relations || tags {
                    node = r.into_typed::<DbNode>()?;
                } else {
                    let simple = r.into_typed::<DbNodeSimple>()?;
                    node = DbNode::from_simple(simple);
                }
                ret.push(node);
            }
        }

        let elapsed = now.elapsed();
        info!(
            "ScyllaDbService: get_node: Got node {}. Took {:.2?}",
            id, elapsed
        );

        Ok(ret)
    }

    pub async fn save_nodes(
        &self,
        entries: Vec<DbNode>,
    ) -> Result<(), Box<dyn std::error::Error + Sync + Send>> {
        let now = Instant::now();
        let sem = Arc::new(Semaphore::new(self.parallelism));
        info!("ScyllaDbService: save_nodes: Saving Nodes...");

        let mut i = 0;
        let mut handlers: Vec<JoinHandle<_>> = Vec::new();
        for entry in entries {
            let session = self.db_session.clone();
            let prepared = self.ps.clone();
            let permit = sem.clone().acquire_owned().await;
            debug!("save_nodes: Creating Task...");
            handlers.push(tokio::task::spawn(async move {
                debug!("save_nodes: Running query for node {}", entry.name);
                let result = session
                    .execute(
                        &prepared,
                        (
                            entry.uuid,
                            entry.direction.unwrap_or_default(),
                            entry.relation.unwrap_or_default(),
                            entry.relates_to.unwrap_or_default(),
                            entry.name,
                            entry.ingestion_id,
                            entry.url,
                            entry.node_type,
                            entry.tags.unwrap_or_default(),
                        ),
                    )
                    .await;

                let _permit = permit;

                result
            }));
            debug!("save_nodes: Task Created");
            i += 1;
        }

        info!(
            "ScyllaDbService: save_nodes: Waiting for {} tasks to complete...",
            i
        );

        let mut error_count = 0;
        for thread in handlers {
            match thread.await? {
                Err(e) => {
                    error!("save_nodes: Error Executing Query. {:?}", e);
                    error_count += 1;
                }
                Ok(r) => debug!("save_nodes: Query Result: {:?}", r),
            };
        }

        let elapsed = now.elapsed();
        info!(
            "ScyllaDbService: save_nodes: {} save nodes tasks completed. ERRORS: {}. Took: {:.2?}",
            i, error_count, elapsed
        );
        Ok(())
    }
}
