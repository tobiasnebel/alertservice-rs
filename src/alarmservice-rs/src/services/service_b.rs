use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error, sync::Arc};

use crate::{
    core_logic::{
        state::{RefKeyType, ServiceSnapshot, StateValue},
        traits::SnapshotStorage as SnapshotStorageTrait,
    },
    persistence::entities::service_b_snapshot, // Changed from service_a_snapshot
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ServiceBStateData {
    pub value_sum: i64, // Example: different field than ServiceA
    pub last_event_time: Option<DateTime<Utc>>,
}

impl StateValue for ServiceBStateData {}

pub struct ServiceBSnapshotStore {
    db_conn: Arc<DatabaseConnection>,
}

impl ServiceBSnapshotStore {
    pub fn new(db_conn: Arc<DatabaseConnection>) -> Self {
        Self { db_conn }
    }
}

#[async_trait]
impl SnapshotStorageTrait<ServiceBStateData> for ServiceBSnapshotStore {
    async fn save_snapshot(
        &self,
        snapshot: &ServiceSnapshot<ServiceBStateData>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (ref_id, state_value) in &snapshot.state {
            let model = service_b_snapshot::ActiveModel { // Changed entity
                ref_id: sea_orm::Set(ref_id.clone()),
                snapshot_time: sea_orm::Set(snapshot.timestamp),
                state_data: sea_orm::Set(serde_json::to_value(state_value)?),
                ..Default::default()
            };

            let insert_result = service_b_snapshot::Entity::insert(model.clone()) // Changed entity
                .on_conflict(
                    sea_orm::sea_query::OnConflict::columns([
                        service_b_snapshot::Column::RefId, // Changed column
                        service_b_snapshot::Column::SnapshotTime, // Changed column
                    ])
                    .update_column(service_b_snapshot::Column::StateData) // Changed column
                    .to_owned(),
                )
                .exec(self.db_conn.as_ref())
                .await;
            
            if let Err(e) = insert_result {
                 // Log or handle error, e.g. if it's not a unique constraint violation
                 eprintln!("Error during ServiceB save_snapshot upsert: {:?}", e);
            }
        }
        Ok(())
    }

    async fn load_latest_snapshot_before(
        &self,
        ref_id: &RefKeyType,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<ServiceSnapshot<ServiceBStateData>>, Box<dyn Error + Send + Sync>> {
        let snapshot_model = service_b_snapshot::Entity::find() // Changed entity
            .filter(service_b_snapshot::Column::RefId.eq(ref_id.clone())) // Changed column
            .filter(service_b_snapshot::Column::SnapshotTime.lte(timestamp)) // Changed column
            .order_by_desc(service_b_snapshot::Column::SnapshotTime) // Changed column
            .one(self.db_conn.as_ref())
            .await?;

        if let Some(model) = snapshot_model {
            let state_value: ServiceBStateData = serde_json::from_value(model.state_data)?;
            let mut state_map = HashMap::new();
            state_map.insert(model.ref_id, state_value);
            Ok(Some(ServiceSnapshot {
                timestamp: model.snapshot_time,
                state: state_map,
            }))
        } else {
            Ok(None)
        }
    }

    async fn delete_snapshots_after(
        &self,
        ref_id: &RefKeyType,
        timestamp: DateTime<Utc>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        service_b_snapshot::Entity::delete_many() // Changed entity
            .filter(service_b_snapshot::Column::RefId.eq(ref_id.clone())) // Changed column
            .filter(service_b_snapshot::Column::SnapshotTime.gt(timestamp)) // Changed column
            .exec(self.db_conn.as_ref())
            .await?;
        Ok(())
    }
}

// --- Implementation of ServiceB struct and its traits ---
use crate::{
    core_logic::state::{InternalState, RefKeyType, ServiceSnapshot},
    core_logic::traits::{EventProcessor, HistoricalDataProvider, StateManager},
    models::models::{
        processed_event_dto::ProcessedEventDto, service_event_dto::ServiceBRelevantEvent,
    },
    persistence::entities::event_log,
    services::service_a::ServiceAStateData, // Added for HistoricalDataProvider field
};
use chrono::{DateTime, Duration, Utc}; // Added Utc and DateTime
use sea_orm::{Set};
use serde_json;
use std::sync::{Mutex};

pub struct ServiceB {
    state: Arc<Mutex<InternalState<ServiceBStateData>>>,
    db_conn: Arc<DatabaseConnection>,
    snapshot_storage: Arc<ServiceBSnapshotStore>,
    service_a_provider: Option<
        Arc<dyn HistoricalDataProvider<(), Option<ServiceAStateData>> + Send + Sync>,
    >,
}

impl ServiceB {
    pub fn new(db_conn: Arc<DatabaseConnection>) -> Self {
        Self {
            state: Arc::new(Mutex::new(InternalState::new())),
            snapshot_storage: Arc::new(ServiceBSnapshotStore::new(db_conn.clone())),
            db_conn,
            service_a_provider: None,
        }
    }

    pub fn set_service_a_provider(
        &mut self,
        provider: Arc<dyn HistoricalDataProvider<(), Option<ServiceAStateData>> + Send + Sync>,
    ) {
        self.service_a_provider = Some(provider);
    }

    fn transform_event_log_to_processed_event(
        &self,
        event_log_model: &event_log::Model,
    ) -> Result<ProcessedEventDto, Box<dyn Error + Send + Sync>> {
        Ok(ProcessedEventDto {
            event_type: event_log_model.event_type.clone(),
            room_id: event_log_model.room_id,
            timestamp: event_log_model.timestamp,
            ref_id: event_log_model.ref_id.clone(),
            original_payload: event_log_model.raw_event.clone(),
        })
    }

    fn apply_relevant_event_to_state_value(
        &self,
        mut current_data: ServiceBStateData,
        relevant_event: &ServiceBRelevantEvent,
    ) -> ServiceBStateData {
        current_data.value_sum += relevant_event.value as i64;
        current_data.last_event_time = Some(relevant_event.timestamp);
        current_data
    }
}

#[async_trait]
impl EventProcessor<ServiceBRelevantEvent> for ServiceB {
    async fn process_event(
        &self,
        event: ProcessedEventDto,
    ) -> Result<Option<ServiceBRelevantEvent>, Box<dyn Error + Send + Sync>> {
        // Example: Process only events with event_type "TYPE_B" or if original_payload contains a specific value
        if event.event_type == "SERVICE_B_RELEVANT_EVENT" {
             // Attempt to parse 'value' from original_payload.
            // This is a placeholder for actual payload parsing logic.
            let value_from_payload = event.original_payload.get("value_field").and_then(|v| v.as_i64()).unwrap_or(0);

            Ok(Some(ServiceBRelevantEvent {
                timestamp: event.timestamp,
                ref_id: event.ref_id,
                value: value_from_payload as i32, // Assuming 'value' is i32 in ServiceBRelevantEvent
            }))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl StateManager<ServiceBStateData, ServiceBRelevantEvent> for ServiceB {
    async fn apply_event(
        &self,
        event: ServiceBRelevantEvent,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut state_lock = self.state.lock().map_err(|e| e.to_string())?;
        let current_data = state_lock
            .get(&event.ref_id)
            .cloned()
            .unwrap_or_default();

        let updated_data = self.apply_relevant_event_to_state_value(current_data, &event);
        state_lock.insert(event.ref_id.clone(), updated_data);
        Ok(())
    }

    async fn get_current_state_for_ref_id(
        &self,
        ref_id: &RefKeyType,
    ) -> Result<Option<ServiceBStateData>, Box<dyn Error + Send + Sync>> {
        let state_lock = self.state.lock().map_err(|e| e.to_string())?;
        Ok(state_lock.get(ref_id).cloned())
    }

    async fn create_snapshot(
        &self,
    ) -> Result<ServiceSnapshot<ServiceBStateData>, Box<dyn Error + Send + Sync>> {
        let state_lock = self.state.lock().map_err(|e| e.to_string())?;
        Ok(ServiceSnapshot {
            timestamp: Utc::now(),
            state: state_lock.clone(),
        })
    }

    async fn restore_from_snapshot(
        &self,
        snapshot: ServiceSnapshot<ServiceBStateData>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut state_lock = self.state.lock().map_err(|e| e.to_string())?;
        *state_lock = snapshot.state;
        Ok(())
    }

    async fn handle_late_event(
        &self,
        event_timestamp: DateTime<Utc>,
        ref_id: &RefKeyType,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        self.snapshot_storage
            .delete_snapshots_after(ref_id, event_timestamp)
            .await?;
        Ok(())
    }

    async fn get_state_at_timestamp(
        &self,
        ref_id: &RefKeyType,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<ServiceBStateData>, Box<dyn Error + Send + Sync>> {
        let latest_snapshot_opt = self
            .snapshot_storage
            .load_latest_snapshot_before(ref_id, timestamp)
            .await?;

        let (mut current_processing_state, start_time_for_event_query) =
            if let Some(snapshot) = latest_snapshot_opt {
                (
                    snapshot.state.get(ref_id).cloned().unwrap_or_default(),
                    snapshot.timestamp,
                )
            } else {
                (
                    ServiceBStateData::default(),
                    Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(),
                )
            };

        let events_to_replay = event_log::Entity::find()
            .filter(event_log::Column::RefId.eq(ref_id.clone()))
            .filter(event_log::Column::Timestamp.gt(start_time_for_event_query))
            .filter(event_log::Column::Timestamp.lte(timestamp))
            .order_by_asc(event_log::Column::Timestamp)
            .all(self.db_conn.as_ref())
            .await?;

        for event_log_model in events_to_replay {
            let processed_event =
                self.transform_event_log_to_processed_event(&event_log_model)?;
            if let Some(relevant_event) = self.process_event(processed_event).await? {
                 current_processing_state = self.apply_relevant_event_to_state_value(current_processing_state, &relevant_event);
            }
        }
        Ok(Some(current_processing_state))
    }
}

#[async_trait]
impl HistoricalDataProvider<(), Option<ServiceBStateData>> for ServiceB {
    async fn get_historical_data(
        &self,
        ref_id: &RefKeyType,
        timestamp: DateTime<Utc>,
        _request_details: (),
    ) -> Result<Option<ServiceBStateData>, Box<dyn Error + Send + Sync>> {
        self.get_state_at_timestamp(ref_id, timestamp).await
    }
}
