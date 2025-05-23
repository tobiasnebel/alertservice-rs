use std::collections::HashMap;
use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

pub type RefKeyType = String;

pub trait StateValue: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + std::fmt::Debug + 'static {}

impl<T: Clone + Send + Sync + Serialize + for<'de> Deserialize<'de> + std::fmt::Debug + 'static> StateValue for T {}

pub type InternalState<V: StateValue> = HashMap<RefKeyType, V>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceSnapshot<V: StateValue> {
    pub timestamp: DateTime<Utc>,
    pub state: InternalState<V>,
}
