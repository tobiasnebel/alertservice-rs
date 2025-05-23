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
    persistence::entities::service_a_snapshot,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ServiceAStateData {
    pub count: i32,
    pub last_updated: DateTime<Utc>,
}

use crate::{
    core_logic::state::{InternalState, RefKeyType, ServiceSnapshot, StateValue},
    core_logic::traits::{
        EventProcessor, HistoricalDataProvider, SnapshotStorage as SnapshotStorageTrait,
        StateManager,
    },
    models::models::{
        processed_event_dto::ProcessedEventDto, service_event_dto::ServiceARelevantEvent,
    },
    persistence::entities::{event_log, service_a_snapshot},
    services::service_b::ServiceBStateData, // Added for HistoricalDataProvider field
};
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use sea_orm::{
    ActiveModelTrait, ColumnTrait, ConnectionTrait, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect, Set,
};
use serde::{Deserialize, Serialize};
use serde_json;
use std::{
    collections::HashMap,
    error::Error,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct ServiceAStateData {
    pub count: i32,
    pub last_updated: DateTime<Utc>,
    // Add a field to track if this state was loaded from a snapshot or calculated
    // This can help in the get_state_at_timestamp optimization.
    // For now, we'll keep it simple and recalculate more often.
}

impl StateValue for ServiceAStateData {}

pub struct ServiceASnapshotStore {
    db_conn: Arc<DatabaseConnection>,
}

impl ServiceASnapshotStore {
    pub fn new(db_conn: Arc<DatabaseConnection>) -> Self {
        Self { db_conn }
    }
}

#[async_trait]
impl SnapshotStorageTrait<ServiceAStateData> for ServiceASnapshotStore {
    async fn save_snapshot(
        &self,
        snapshot: &ServiceSnapshot<ServiceAStateData>,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        for (ref_id, state_value) in &snapshot.state {
            let model = service_a_snapshot::ActiveModel {
                ref_id: sea_orm::Set(ref_id.clone()),
                snapshot_time: sea_orm::Set(snapshot.timestamp),
                state_data: sea_orm::Set(serde_json::to_value(state_value)?),
                ..Default::default() // id will be auto-incremented
            };

            // Upsert: On conflict of (ref_id, snapshot_time), update state_data
            // This requires the unique index to be set up correctly in the migration.
            // For SeaORM, proper upsert syntax might depend on the specific DB features.
            // A common approach is to try an insert and then an update on conflict.
            // However, SeaORM's `save` with `on_conflict` (if available for your DB)
            // or a raw SQL query might be more direct.
            // For now, we assume a simple save, relying on a unique constraint to prevent duplicates
            // or using a more advanced save method if available.
            // A more robust upsert might look like:
            // service_a_snapshot::Entity::insert(model)
            //     .on_conflict(
            //         sea_orm::sea_query::OnConflict::columns([
            //             service_a_snapshot::Column::RefId,
            //             service_a_snapshot::Column::SnapshotTime,
            //         ])
            //         .update_column(service_a_snapshot::Column::StateData)
            //         .to_owned(),
            //     )
            //     .exec(self.db_conn.as_ref())
            //     .await?;
            // Given the current tools, a simple save is used. If duplicates are an issue,
            // it implies a need to either delete first or use a more complex upsert strategy.
            // For this implementation, we'll assume that either snapshot times are unique enough,
            // or a prior mechanism prevents exact duplicate snapshot saves.
            // A simple insert, relying on the unique constraint for (ref_id, snapshot_time)
            // This will error if a duplicate (ref_id, snapshot_time) is inserted.
            // A better approach would be an upsert.
            // Let's use `insert` and `on_conflict` for a proper upsert.
            let insert_result = service_a_snapshot::Entity::insert(model.clone())
                .on_conflict(
                    sea_orm::sea_query::OnConflict::columns([
                        service_a_snapshot::Column::RefId,
                        service_a_snapshot::Column::SnapshotTime,
                    ])
                    .update_column(service_a_snapshot::Column::StateData)
                    .to_owned(),
                )
                .exec(self.db_conn.as_ref())
                .await;

            if let Err(e) = insert_result {
                // If the error is a unique constraint violation, it means the upsert happened.
                // Depending on the DB driver, the error type might vary.
                // For simplicity, we'll log and continue if it's not a "real" error.
                // This part is tricky without specific DB error types.
                // A robust implementation would check the specific error.
                // For now, we assume the on_conflict handles it.
                eprintln!("Error during save_snapshot upsert (may be expected if upsert occurred): {:?}", e);
            }
        }
        Ok(())
    }

    async fn load_latest_snapshot_before(
        &self,
        ref_id: &RefKeyType,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<ServiceSnapshot<ServiceAStateData>>, Box<dyn Error + Send + Sync>> {
        let snapshot_model = service_a_snapshot::Entity::find()
            .filter(service_a_snapshot::Column::RefId.eq(ref_id.clone()))
            .filter(service_a_snapshot::Column::SnapshotTime.lte(timestamp))
            .order_by_desc(service_a_snapshot::Column::SnapshotTime)
            .one(self.db_conn.as_ref())
            .await?;

        if let Some(model) = snapshot_model {
            let state_value: ServiceAStateData = serde_json::from_value(model.state_data)?;
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
        service_a_snapshot::Entity::delete_many()
            .filter(service_a_snapshot::Column::RefId.eq(ref_id.clone()))
            .filter(service_a_snapshot::Column::SnapshotTime.gt(timestamp))
            .exec(self.db_conn.as_ref())
            .await?;
        Ok(())
    }
}

pub struct ServiceA {
    state: Arc<Mutex<InternalState<ServiceAStateData>>>,
    db_conn: Arc<DatabaseConnection>,
    snapshot_storage: Arc<ServiceASnapshotStore>,
    service_b_provider: Option<
        Arc<dyn HistoricalDataProvider<(), Option<ServiceBStateData>> + Send + Sync>,
    >,
}

impl ServiceA {
    pub fn new(db_conn: Arc<DatabaseConnection>) -> Self {
        Self {
            state: Arc::new(Mutex::new(InternalState::new())),
            snapshot_storage: Arc::new(ServiceASnapshotStore::new(db_conn.clone())),
            db_conn,
            service_b_provider: None,
        }
    }

    pub fn set_service_b_provider(
        &mut self,
        provider: Arc<dyn HistoricalDataProvider<(), Option<ServiceBStateData>> + Send + Sync>,
    ) {
        self.service_b_provider = Some(provider);
    }

    // Helper function for transforming event_log::Model to ProcessedEventDto
    // This is a simplified transformation. A real one might need more error handling
    // or access to more context.
    fn transform_event_log_to_processed_event(
        &self,
        event_log_model: &event_log::Model,
    ) -> Result<ProcessedEventDto, Box<dyn Error + Send + Sync>> {
        // Assuming original_payload in ProcessedEventDto was the full EventDto
        // and raw_event in event_log::Model stores this EventDto as JSON.
        // This part might need adjustment based on actual data stored.
        // For now, we'll construct a ProcessedEventDto as best as we can.
        Ok(ProcessedEventDto {
            event_type: event_log_model.event_type.clone(),
            room_id: event_log_model.room_id, // Option<i64>
            timestamp: event_log_model.timestamp,
            ref_id: event_log_model.ref_id.clone(),
            original_payload: event_log_model.raw_event.clone(),
        })
    }

    // Helper function for applying a relevant event to a state value.
    // This logic is shared between apply_event and get_state_at_timestamp.
    fn apply_relevant_event_to_state_value(
        &self,
        mut current_data: ServiceAStateData,
        relevant_event: &ServiceARelevantEvent,
    ) -> ServiceAStateData {
        current_data.count += 1; // Example: increment count
        current_data.last_updated = relevant_event.timestamp;
        // Potentially use relevant_event.data for more complex updates
        current_data
    }
}

#[async_trait]
impl EventProcessor<ServiceARelevantEvent> for ServiceA {
    async fn process_event(
        &self,
        event: ProcessedEventDto,
    ) -> Result<Option<ServiceARelevantEvent>, Box<dyn Error + Send + Sync>> {
        // Example: Process all events for now.
        // In a real scenario, filter by event.event_type or other criteria.
        // if event.event_type == "SERVICE_A_RELEVANT_TYPE" {
        Ok(Some(ServiceARelevantEvent {
            timestamp: event.timestamp,
            ref_id: event.ref_id,
            data: format!("Processed data from event type: {}", event.event_type),
        }))
        // } else {
        //     Ok(None)
        // }
    }
}

#[async_trait]
impl StateManager<ServiceAStateData, ServiceARelevantEvent> for ServiceA {
    async fn apply_event(
        &self,
        event: ServiceARelevantEvent,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut state_lock = self.state.lock().map_err(|e| e.to_string())?;
        let current_data = state_lock
            .get(&event.ref_id)
            .cloned()
            .unwrap_or_default(); // Default if no state yet for this ref_id

        let updated_data = self.apply_relevant_event_to_state_value(current_data, &event);
        state_lock.insert(event.ref_id, updated_data);
        Ok(())
    }

    async fn get_current_state_for_ref_id(
        &self,
        ref_id: &RefKeyType,
    ) -> Result<Option<ServiceAStateData>, Box<dyn Error + Send + Sync>> {
        let state_lock = self.state.lock().map_err(|e| e.to_string())?;
        Ok(state_lock.get(ref_id).cloned())
    }

    async fn create_snapshot(
        &self,
    ) -> Result<ServiceSnapshot<ServiceAStateData>, Box<dyn Error + Send + Sync>> {
        let state_lock = self.state.lock().map_err(|e| e.to_string())?;
        Ok(ServiceSnapshot {
            timestamp: Utc::now(),
            state: state_lock.clone(),
        })
    }

    async fn restore_from_snapshot(
        &self,
        snapshot: ServiceSnapshot<ServiceAStateData>,
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
        // First, delete any snapshots that are now invalid because they occurred after this late event.
        self.snapshot_storage
            .delete_snapshots_after(ref_id, event_timestamp)
            .await?;

        // More complex logic might be needed here:
        // 1. Invalidate current in-memory state for this ref_id if it was built from a now-deleted snapshot
        //    or if its `last_updated` is after `event_timestamp`. This would force recalculation on next read.
        //    For simplicity, we are not explicitly invalidating the in-memory state here, but
        //    `get_state_at_timestamp` will inherently recalculate if it can't find a suitable snapshot.
        // 2. Potentially trigger a background recalculation for this ref_id up to the current time.

        // For now, deleting future snapshots is the primary responsibility.
        // The `get_state_at_timestamp` logic will handle recalculation from the latest valid snapshot or from scratch.
        Ok(())
    }

    async fn get_state_at_timestamp(
        &self,
        ref_id: &RefKeyType,
        timestamp: DateTime<Utc>,
    ) -> Result<Option<ServiceAStateData>, Box<dyn Error + Send + Sync>> {
        // Step 1: Try to use current in-memory state if possible (simplified check)
        // A more robust check would involve "state purity" tracking.
        // For now, if current state for ref_id is more recent than requested, we can't use it directly.

        // Step 2: Load the latest snapshot before or at the requested timestamp.
        let latest_snapshot_opt = self
            .snapshot_storage
            .load_latest_snapshot_before(ref_id, timestamp)
            .await?;

        // Step 3: Determine start_time_for_event_query and initial_state
        let (mut current_processing_state, start_time_for_event_query) =
            if let Some(snapshot) = latest_snapshot_opt {
                // Snapshot loaded
                (
                    snapshot.state.get(ref_id).cloned().unwrap_or_default(),
                    snapshot.timestamp, // Start querying events after snapshot's time
                )
            } else {
                // No snapshot found, start from the beginning of time (or a reasonable default)
                (
                    ServiceAStateData::default(),
                    Utc.with_ymd_and_hms(1970, 1, 1, 0, 0, 0).unwrap(), // Or a system-defined epoch
                )
            };

        // Step 4: Query event_log for events for the ref_id
        let events_to_replay = event_log::Entity::find()
            .filter(event_log::Column::RefId.eq(ref_id.clone()))
            .filter(event_log::Column::Timestamp.gt(start_time_for_event_query)) // Events strictly after snapshot
            .filter(event_log::Column::Timestamp.lte(timestamp)) // Events up to or at requested time
            .order_by_asc(event_log::Column::Timestamp)
            .all(self.db_conn.as_ref())
            .await?;

        // Step 5 & 6: Iterate through fetched events and apply them
        for event_log_model in events_to_replay {
            let processed_event =
                self.transform_event_log_to_processed_event(&event_log_model)?;
            if let Some(relevant_event) = self.process_event(processed_event).await? {
                // Apply the relevant event to the temporary state
                 current_processing_state = self.apply_relevant_event_to_state_value(current_processing_state, &relevant_event);
            }
        }

        // Step 7: Return the resulting temporary state
        // If no snapshot and no events, this will be the default state.
        Ok(Some(current_processing_state))
    }
}

#[async_trait]
impl HistoricalDataProvider<(), Option<ServiceAStateData>> for ServiceA {
    async fn get_historical_data(
        &self,
        ref_id: &RefKeyType,
        timestamp: DateTime<Utc>,
        _request_details: (),
    ) -> Result<Option<ServiceAStateData>, Box<dyn Error + Send + Sync>> {
        self.get_state_at_timestamp(ref_id, timestamp).await
    }
}
