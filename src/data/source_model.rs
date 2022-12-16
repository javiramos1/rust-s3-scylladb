// SOURCE SCHEMA
use serde::Deserialize;
use serde::Serialize;

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct File {
    pub nodes: Vec<Nodes>,
    pub relations: Vec<Relation>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Nodes {
    pub name: String,
    #[serde(rename = "type")]
    pub type_field: String,
    pub children: Vec<Nodes>,
    pub tags: Option<Vec<Tag>>,
    pub total_children: Option<i64>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Tag {
    #[serde(rename = "type")]
    pub type_field: String,
    pub value: String,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Relation {
    #[serde(rename = "type")]
    pub type_field: String,
    pub source: Vec<String>,
    pub target: Vec<String>,
    pub tags: Option<Vec<Tag>>,
}
