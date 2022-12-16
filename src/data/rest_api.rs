use serde::Serialize;
use serde::Deserialize;

#[derive(Serialize, Deserialize)]
pub struct IngestionRequest {
    pub ingestion_id: String,
    pub files: Vec<String>
}

#[derive(Serialize, Deserialize)]
#[serde(transparent)]
pub struct File {
    pub files: Vec<String>
}


#[derive(Debug, Serialize)]
pub struct AppError {
    message: String
}

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
pub struct GetNodeRequest {
    pub get_tags: Option<bool>,
    pub get_relations: Option<bool>
}

#[derive(Debug, Serialize, Clone, Deserialize, Default)]
pub struct TraversalNodeRequest {
    pub direction: String,
    pub relation_type: Option<String>,
    pub max_depth: usize
}