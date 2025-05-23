use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceARelevantEvent {
    pub timestamp: DateTime<Utc>,
    pub ref_id: String,
    pub data: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceBRelevantEvent {
    pub timestamp: DateTime<Utc>,
    pub ref_id: String,
    pub value: i32,
}
