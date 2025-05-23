use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create ServiceASnapshot table
        manager
            .create_table(
                Table::create()
                    .table(ServiceASnapshot::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ServiceASnapshot::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ServiceASnapshot::RefId).string().not_null())
                    .col(
                        ColumnDef::new(ServiceASnapshot::SnapshotTime)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ServiceASnapshot::StateData).json().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx-service_a_snapshot-ref_id-snapshot_time_desc")
                    .table(ServiceASnapshot::Table)
                    .col(ServiceASnapshot::RefId)
                    .col((ServiceASnapshot::SnapshotTime, IndexOrder::Desc))
                    .unique() // Enforce unique constraint for (ref_id, snapshot_time)
                    .to_owned(),
            )
            .await?;

        // Create ServiceBSnapshot table
        manager
            .create_table(
                Table::create()
                    .table(ServiceBSnapshot::Table)
                    .if_not_exists()
                    .col(
                        ColumnDef::new(ServiceBSnapshot::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(ServiceBSnapshot::RefId).string().not_null())
                    .col(
                        ColumnDef::new(ServiceBSnapshot::SnapshotTime)
                            .timestamp_with_time_zone()
                            .not_null(),
                    )
                    .col(ColumnDef::new(ServiceBSnapshot::StateData).json().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("idx-service_b_snapshot-ref_id-snapshot_time_desc")
                    .table(ServiceBSnapshot::Table)
                    .col(ServiceBSnapshot::RefId)
                    .col((ServiceBSnapshot::SnapshotTime, IndexOrder::Desc))
                    .unique() // Enforce unique constraint for (ref_id, snapshot_time)
                    .to_owned(),
            )
            .await
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(ServiceASnapshot::Table).to_owned())
            .await?;
        manager
            .drop_table(Table::drop().table(ServiceBSnapshot::Table).to_owned())
            .await
    }
}

#[derive(DeriveIden)]
enum ServiceASnapshot {
    Table,
    Id,
    RefId,
    SnapshotTime,
    StateData,
}

#[derive(DeriveIden)]
enum ServiceBSnapshot {
    Table,
    Id,
    RefId,
    SnapshotTime,
    StateData,
}
