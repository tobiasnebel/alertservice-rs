/*
 * Alarmservice Demo
 *
 * No description provided (generated by Openapi Generator https://github.com/openapitools/openapi-generator)
 *
 * The version of the OpenAPI document: 1.0.0
 * 
 * Generated by: https://openapi-generator.tech
 */

// Tsify generates non-snake-case methods. We need to allow that here in order to keep clippy from escalating.
#![allow(non_snake_case)]

use crate::models;

//use serde::{Deserialize, Serialize};
use serde_derive::Serialize;
use serde_derive::Deserialize;

use tsify_next::Tsify;

#[derive(Tsify, Clone, Default, Debug, PartialEq, Serialize, Deserialize)]
#[tsify(into_wasm_abi, from_wasm_abi)]
pub struct EventDto {
    #[serde(rename = "eventType")]
    
    pub event_type: String,
    #[serde(rename = "roomId")]
    
    pub room_id: i64,
    #[serde(rename = "timestamp", skip_serializing_if = "Option::is_none")]
    
    pub timestamp: Option<String>,
}

impl EventDto {
    pub fn new(event_type: String, room_id: i64) -> EventDto {
        EventDto {
            event_type,
            room_id,
            timestamp: None,
        }
    }
}

