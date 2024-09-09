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
pub struct ScheduleDto {
    #[serde(rename = "begin", skip_serializing_if = "Option::is_none")]
    
    pub begin: Option<i32>,
    #[serde(rename = "end", skip_serializing_if = "Option::is_none")]
    
    pub end: Option<i32>,
    #[serde(rename = "days_of_week_mask", skip_serializing_if = "Option::is_none")]
    
    pub days_of_week_mask: Option<i32>,
    #[serde(rename = "roomId", skip_serializing_if = "Option::is_none")]
    
    pub room_id: Option<i64>,
}

impl ScheduleDto {
    pub fn new() -> ScheduleDto {
        ScheduleDto {
            begin: None,
            end: None,
            days_of_week_mask: None,
            room_id: None,
        }
    }
}

