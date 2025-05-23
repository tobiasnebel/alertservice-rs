use async_trait::async_trait;
use chrono::{DateTime, Utc};
use super::state::{ServiceSnapshot, RefKeyType, StateValue};
use crate::models::models::processed_event_dto::ProcessedEventDto; 
use std::error::Error;

#[async_trait]
pub trait EventProcessor<RelevantEvent> {
    async fn process_event(&self, event: ProcessedEventDto) -> Result<Option<RelevantEvent>, Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait StateManager<V: StateValue, RelevantEvent>: Send + Sync {
    async fn apply_event(&self, event: RelevantEvent) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn get_current_state_for_ref_id(&self, ref_id: &RefKeyType) -> Result<Option<V>, Box<dyn Error + Send + Sync>>;
    async fn get_state_at_timestamp(&self, ref_id: &RefKeyType, timestamp: DateTime<Utc>) -> Result<Option<V>, Box<dyn Error + Send + Sync>>;
    async fn create_snapshot(&self) -> Result<ServiceSnapshot<V>, Box<dyn Error + Send + Sync>>;
    async fn restore_from_snapshot(&self, snapshot: ServiceSnapshot<V>) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn handle_late_event(&self, event_timestamp: DateTime<Utc>, ref_id: &RefKeyType) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait SnapshotStorage<V: StateValue>: Send + Sync {
    async fn save_snapshot(&self, snapshot: &ServiceSnapshot<V>) -> Result<(), Box<dyn Error + Send + Sync>>;
    async fn load_latest_snapshot_before(&self, ref_id: &RefKeyType, timestamp: DateTime<Utc>) -> Result<Option<ServiceSnapshot<V>>, Box<dyn Error + Send + Sync>>;
    async fn delete_snapshots_after(&self, ref_id: &RefKeyType, timestamp: DateTime<Utc>) -> Result<(), Box<dyn Error + Send + Sync>>;
}

#[async_trait]
pub trait HistoricalDataProvider<RequestValue: Send, ResponseValue: Send>: Send + Sync {
    async fn get_historical_data(&self, ref_id: &RefKeyType, timestamp: DateTime<Utc>, request_details: RequestValue) -> Result<Option<ResponseValue>, Box<dyn Error + Send + Sync>>;
}
