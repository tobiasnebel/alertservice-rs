use sea_orm_migration::{async_trait, MigrationTrait, MigratorTrait};

use sea_orm_migration::{async_trait, MigrationTrait, MigratorTrait};

use sea_orm_migration::{async_trait, MigrationTrait, MigratorTrait};

pub mod v1_001_create_tables;
pub mod v1_002_add_data;
pub mod v1_003_create_event_log_table;
pub mod v1_004_create_snapshot_tables;

pub struct Migrator;

#[async_trait::async_trait]
impl MigratorTrait for Migrator {
    fn migrations() -> Vec<Box<dyn MigrationTrait>> {
        vec![
            Box::new(v1_001_create_tables::Migration),
            Box::new(v1_002_add_data::Migration),
            Box::new(v1_003_create_event_log_table::Migration),
            Box::new(v1_004_create_snapshot_tables::Migration),
        ]
    }
}
