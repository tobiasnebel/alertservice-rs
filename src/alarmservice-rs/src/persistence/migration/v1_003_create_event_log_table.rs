use sea_orm_migration::prelude::*;

use super::m20220101_000001_create_table::Room;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(EventLog::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(EventLog::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(EventLog::EventType).string().not_null())
                    .col(ColumnDef::new(EventLog::RoomId).big_integer())
                    .col(
                        ColumnDef::new(EventLog::Timestamp)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(ColumnDef::new(EventLog::RefId).string().not_null())
                    .col(ColumnDef::new(EventLog::RawEvent).json().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx-event_log-timestamp")
                    .table(EventLog::Table)
                    .col(EventLog::Timestamp)
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx-event_log-ref_id-timestamp")
                    .table(EventLog::Table)
                    .col(EventLog::RefId)
                    .col(EventLog::Timestamp)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(EventLog::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum EventLog {
    Table,
    Id,
    EventType,
    RoomId,
    Timestamp,
    RefId,
    RawEvent,
}

// We need to link to the Room table if we want to add a foreign key constraint.
// In this case, we are not adding a foreign key constraint for room_id in event_log,
// as room_id might be None or refer to entities not in the Room table in a more complex system.
// If a foreign key was desired:
// .add_foreign_key(
// TableForeignKey::new()
// .name("fk-event_log-room_id")
// .from_tbl(EventLog::Table)
// .from_col(EventLog::RoomId)
// .to_tbl(Room::Table) // Ensure Room enum is imported from the relevant migration
// .to_col(Room::Id)
// .on_delete(ForeignKeyAction::SetNull) // Or Cascade, Restrict as needed
// .on_update(ForeignKeyAction::Cascade)
// )
