mod api;
mod config;
mod metrics;
mod persistence;
mod tracing;
pub mod core_logic; // Ensure core_logic is public if services need its traits directly
pub mod services;   // Ensure services is public

use std::{net::SocketAddr, sync::Arc}; // Added Arc

use ::tracing::{info, error, warn}; // Added error and warn
use api::router::{router, AppState};
use axum_prometheus::metrics_exporter_prometheus::PrometheusBuilder;
use tokio::sync::{mpsc, Mutex}; // Added mpsc and tokio::sync::Mutex
use tokio::time::{self, Duration}; // Added time and Duration

use crate::{
    services::service_a::ServiceA,
    services::service_b::ServiceB,
    core_logic::traits::{EventProcessor, StateManager, SnapshotStorage}, // SnapshotStorage for service internal use
    models::models::processed_event_dto::ProcessedEventDto,
};
use clap::Parser;
use config::AppConfig;
use metrics::AppMetrics;
use persistence::{database::init_connection, migration::Migrator};
use sea_orm_migration::MigratorTrait;
use tokio::net::TcpListener;
use tracing::init_tracing;

#[derive(Parser, Debug)]
struct Args {
    #[arg(short, long, default_value_t = String::from("config/default"))]
    config: String,
}

#[tokio::main]
async fn main() {
    // parse our args and init 'config_file_name' (from default or provided arg)
    let args = Args::parse();

    // parse our app config
    let config = AppConfig::parse(args.config).expect("Parsing config failed!");

    // init tracing
    let directives = &config.logging.levels.join(",");
    init_tracing(directives);

    // init DB
    let db_conn = init_connection(config.postgres).await;

    // run migrations
    Migrator::up(&db_conn, None)
        .await
        .expect("Migrations failed!");

    info!("Initialization done.");

    // initialize AppMetrics
    let metric_handle = PrometheusBuilder::new()
        .add_global_label("app", "alertservice-rs")
        .install_recorder()
        .expect("Failed to initialize prometheus metric handle");
    let app_metrics = AppMetrics::init();

    // Channel Creation using config
    let (tx_service_a, rx_service_a) =
        mpsc::channel::<ProcessedEventDto>(config.service_a.channel_buffer_size);
    let (tx_service_b, rx_service_b) =
        mpsc::channel::<ProcessedEventDto>(config.service_b.channel_buffer_size);

    // Database Connection Arc
    let db_conn_arc = Arc::new(db_conn);

    // Service Instantiation
    // Wrapping services in Arc<Mutex<T>> for shared mutable access across tasks
    let service_a = Arc::new(Mutex::new(ServiceA::new(db_conn_arc.clone())));
    let service_b = Arc::new(Mutex::new(ServiceB::new(db_conn_arc.clone())));

    // Dependency Injection for HistoricalDataProvider
    // ServiceA and ServiceB must implement HistoricalDataProvider trait themselves.
    // The set_service_x_provider methods should accept Arc<Mutex<ServiceY>>
    // or Arc<dyn HistoricalDataProvider<...> + Send + Sync> if type erasure is preferred.
    // Assuming set_service_x_provider expects Arc<Mutex<ServiceY>> which itself impls the trait.
    {
        let mut service_a_locked = service_a.lock().await; // Use tokio's Mutex, so .await
        let mut service_b_locked = service_b.lock().await; // Use tokio's Mutex, so .await
        
        service_a_locked.set_service_b_provider(service_b.clone());
        service_b_locked.set_service_a_provider(service_a.clone());
    }


    // initialize application state and routes
    let state = AppState {
        conn: db_conn_arc, // Use the Arced DB connection
        metrics: app_metrics,
        tx_service_a: tx_service_a.clone(), // Clone sender for AppState
        tx_service_b: tx_service_b.clone(), // Clone sender for AppState
    };
    let router = router(state.clone(), metric_handle); // Clone AppState for the router

    // Service A Event Loop Task
    let service_a_event_handle = service_a.clone();
    let mut rx_a = rx_service_a; // Take ownership of the receiver
    tokio::spawn(async move {
        info!("Service A event loop started");
        while let Some(processed_event) = rx_a.recv().await {
            let mut service_a_locked = service_a_event_handle.lock().await;
            
            // Late event check
            match service_a_locked.get_current_state_for_ref_id(&processed_event.ref_id).await {
                Ok(Some(state_val)) => {
                    if processed_event.timestamp < state_val.last_updated {
                        if let Err(e) = service_a_locked.handle_late_event(processed_event.timestamp, &processed_event.ref_id).await {
                            error!("Service A: Error handling late event for ref_id {}: {}", processed_event.ref_id, e);
                        }
                    }
                },
                Ok(None) => { /* No existing state, not a late event in that sense */ },
                Err(e) => {
                    error!("Service A: Error getting current state for ref_id {}: {}", processed_event.ref_id, e);
                }
            }

            match service_a_locked.process_event(processed_event.clone()).await {
                Ok(Some(relevant_event)) => {
                    if let Err(e) = service_a_locked.apply_event(relevant_event).await {
                        error!("Service A: Error applying relevant event for ref_id {}: {}", processed_event.ref_id, e);
                    }
                }
                Ok(None) => { /* Event not relevant to Service A */ }
                Err(e) => {
                    error!("Service A: Error processing event for ref_id {}: {}", processed_event.ref_id, e);
                }
            }
        }
        info!("Service A event loop ended");
    });

    // Service A Snapshot Task
    let service_a_snapshot_handle = service_a.clone();
    let service_a_snapshot_interval = config.service_a.snapshot_interval_secs; // Get interval from config
    tokio::spawn(async move {
        info!("Service A snapshot task started with interval: {}s", service_a_snapshot_interval);
        let mut interval = time::interval(Duration::from_secs(service_a_snapshot_interval));
        loop {
            interval.tick().await;
            let mut service_a_locked = service_a_snapshot_handle.lock().await;
            match service_a_locked.create_snapshot().await {
                Ok(snapshot) => {
                    // The snapshot_storage is internal to ServiceA, so we call a method on ServiceA
                    // that internally uses its snapshot_storage.
                    // This requires ServiceA to have such a method.
                    // Let's assume `service_a_locked.store_snapshot(snapshot)` or similar.
                    // For now, directly access snapshot_storage if it's pub (not ideal).
                    // The trait SnapshotStorage is on service_a_locked.snapshot_storage.
                    // This part of the prompt seems to assume direct access to snapshot_storage
                    // on the service struct, which means it must be public.
                    // Or, more likely, ServiceA needs a method like `trigger_snapshot_save`.

                    // Assuming ServiceA exposes its snapshot_storage field (or has a method for this)
                    // The prompt was: `service_a_locked.snapshot_storage.save_snapshot(&snapshot).await.ok();`
                    // This implies service_a_locked has a field `snapshot_storage` that implements the trait.
                    // This was how it was structured in a previous subtask.
                    if let Err(e) = service_a_locked.snapshot_storage.save_snapshot(&snapshot).await {
                         error!("Service A: Error saving snapshot: {}", e);
                    } else {
                         info!("Service A: Snapshot created successfully for {} refs", snapshot.state.len());
                    }
                }
                Err(e) => {
                    error!("Service A: Error creating snapshot: {}", e);
                }
            }
        }
    });

    // Service B Event Loop Task
    let service_b_event_handle = service_b.clone();
    let mut rx_b = rx_service_b; // Take ownership
    tokio::spawn(async move {
        info!("Service B event loop started");
        while let Some(processed_event) = rx_b.recv().await {
            let mut service_b_locked = service_b_event_handle.lock().await;

            // Late event check (using last_event_time for ServiceBStateData)
            match service_b_locked.get_current_state_for_ref_id(&processed_event.ref_id).await {
                Ok(Some(state_val)) => {
                    if let Some(last_event_time) = state_val.last_event_time {
                        if processed_event.timestamp < last_event_time {
                             if let Err(e) = service_b_locked.handle_late_event(processed_event.timestamp, &processed_event.ref_id).await {
                                error!("Service B: Error handling late event for ref_id {}: {}", processed_event.ref_id, e);
                            }
                        }
                    }
                },
                Ok(None) => { /* No existing state */ },
                Err(e) => {
                     error!("Service B: Error getting current state for ref_id {}: {}", processed_event.ref_id, e);
                }
            }
            
            match service_b_locked.process_event(processed_event.clone()).await {
                Ok(Some(relevant_event)) => {
                    if let Err(e) = service_b_locked.apply_event(relevant_event).await {
                        error!("Service B: Error applying relevant event for ref_id {}: {}", processed_event.ref_id, e);
                    }
                }
                Ok(None) => { /* Event not relevant to Service B */ }
                Err(e) => {
                    error!("Service B: Error processing event for ref_id {}: {}", processed_event.ref_id, e);
                }
            }
        }
        info!("Service B event loop ended");
    });

    // Service B Snapshot Task
    let service_b_snapshot_handle = service_b.clone();
    let service_b_snapshot_interval = config.service_b.snapshot_interval_secs; // Get interval from config
    tokio::spawn(async move {
        info!("Service B snapshot task started with interval: {}s", service_b_snapshot_interval);
        let mut interval = time::interval(Duration::from_secs(service_b_snapshot_interval));
        loop {
            interval.tick().await;
            let mut service_b_locked = service_b_snapshot_handle.lock().await;
            match service_b_locked.create_snapshot().await {
                Ok(snapshot) => {
                    if let Err(e) = service_b_locked.snapshot_storage.save_snapshot(&snapshot).await {
                        error!("Service B: Error saving snapshot: {}", e);
                    } else {
                        info!("Service B: Snapshot created successfully for {} refs", snapshot.state.len());
                    }
                }
                Err(e) => {
                    error!("Service B: Error creating snapshot: {}", e);
                }
            }
        }
    });

    // start our server
    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.port as u16));
    info!("Listening on port '{}'", config.server.port as u16);
    let listener = TcpListener::bind(&addr)
        .await
        .expect("Failed to initialize tcp listener!");
    axum::serve(listener, router)
        .await
        .expect("Failed to start the server!");
}
