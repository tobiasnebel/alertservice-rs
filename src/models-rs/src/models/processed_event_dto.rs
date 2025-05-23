use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessedEventDto {
    pub event_type: String,
    pub room_id: Option<i64>,
    pub timestamp: DateTime<Utc>,
    pub ref_id: String,
    pub original_payload: Value,
}
