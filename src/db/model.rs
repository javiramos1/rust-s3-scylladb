use scylla::macros::FromRow;
use uuid::Uuid;

use crate::{data::{source_model::Tag, model::{get_id_from_url, Relation}}, DIR};

#[derive(Default, Debug, Clone, FromRow)]
pub struct DbNode {
    pub uuid: Uuid,
    pub direction: Option<String>,
    pub relation: Option<String>,
    pub relates_to: Option<String>,
    pub name: String,
    pub ingestion_id: String,
    pub url: String,
    pub node_type: String,
    pub tags: Option<Vec<(String, String)>>,
}

#[derive(Default, Debug, Clone, FromRow)]
pub struct DbNodeSimple {
    pub uuid: Uuid,
    pub name: String,
    pub node_type: String,
    pub url: String,
    pub ingestion_id: String,
}

#[derive(Default, Debug, Clone, FromRow)]
pub struct DbRelation {
    pub uuid: Uuid,
    pub direction: Option<String>,
    pub relation: Option<String>,
    pub relates_to: Option<String>,
    pub name: String,
    pub node_type: String,
}

impl DbNode {
    pub fn root(
        ingestion_id: String,
        url: String,
        name: String,
        node_type: String,
        source_tags: Vec<Tag>,
    ) -> Self {
        let id = get_id_from_url(ingestion_id.clone(), url.clone());
        let tags: Vec<(String, String)> = source_tags.iter().map(|a| (a.type_field.clone(), a.value.clone())).collect();
  
        Self {
            uuid: id,
            direction: None,
            relation: None,
            relates_to: None,
            name,
            ingestion_id,
            url,
            node_type,
            tags: Some(tags),
        }
    }
    pub fn relation(
        uuid: Uuid,
        ingestion_id: String,
        direction: String,
        relation: String,
        relates_to: String,
        relates_to_name: String,
    ) -> Self {
        Self {
            uuid,
            direction: Some(direction),
            relation: Some(relation),
            relates_to: Some(relates_to),
            name: relates_to_name,
            ingestion_id,
            url: "".to_owned(),
            node_type: "".to_owned(),
            tags: None,
        }
    }
    pub fn from_simple(node: DbNodeSimple) -> Self {
        DbNode {
            uuid: node.uuid,
            direction: None,
            relation: None,
            relates_to: None,
            name: node.name.to_owned(),
            ingestion_id: node.ingestion_id.to_owned(),
            url: node.url.to_owned(),
            node_type: node.node_type,
            tags: None,
        }
    }

    pub fn from_rel(uuid: Uuid, ingestion_id: String, relation: &Relation) -> Self {
        let direction = if relation.outbound {
            DIR::OUT.to_string()
        } else {
            DIR::IN.to_string()
        };
        DbNode {
            uuid,
            direction: Some(direction.to_owned()),
            relation: Some(relation.rel_type.to_owned()),
            relates_to: Some(relation.relates_to.to_owned()),
            name: relation.target_name.to_owned(),
            ingestion_id: ingestion_id,
            url: "".to_owned(),
            node_type: "".to_owned(),
            tags: None,
        }
    }
}
