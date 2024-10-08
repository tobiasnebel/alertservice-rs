use axum::{
    routing::{get, post},
    Router,
};

use axum_prometheus::{metrics_exporter_prometheus::PrometheusHandle, PrometheusMetricLayer};
use sea_orm::DatabaseConnection;

use crate::metrics::AppMetrics;

use super::handlers::{
    alarms_handler::{acknowledge_alarm_handler, get_alarms_for_room_handler},
    event_handler::new_event_handler,
    hello_handler::hello_handler as get_hello_handler,
    rooms_handler::{active_alarm_count_handler, get_rooms_handler},
    schedule_handler::{get_schedules_handler, post_schedule_handler},
};

#[derive(Clone)]
pub struct AppState {
    pub conn: DatabaseConnection,
    pub metrics: AppMetrics,
}

// Router definition
pub fn router(state: AppState, metric_handle: PrometheusHandle) -> Router {
    // routes which should be always publicly accessible
    let routes: Router = Router::new()
        .route("/hello", get(get_hello_handler))
        .route("/room", get(get_rooms_handler))
        .route("/room/:id/alarmcount", get(active_alarm_count_handler))
        .route("/alarm/:id/acknowledge", post(acknowledge_alarm_handler))
        .route("/alarm", get(get_alarms_for_room_handler))
        .route("/event", post(new_event_handler))
        .route("/metrics", get(|| async move { metric_handle.render() }))
        .with_state(state.clone())
        .layer(PrometheusMetricLayer::new());

    let schedules_router = schedules_router(state.clone());

    // merge all routes and return
    routes.merge(schedules_router)
}

fn schedules_router(state: AppState) -> Router {
    Router::new()
        .route("/schedule", get(get_schedules_handler))
        .route("/schedule", post(post_schedule_handler))
        .with_state(state)
}
