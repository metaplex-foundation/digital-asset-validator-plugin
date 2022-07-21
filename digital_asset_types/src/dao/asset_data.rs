//! SeaORM Entity. Generated by sea-orm-codegen 0.8.0

use super::sea_orm_active_enums::ChainMutability;
use super::sea_orm_active_enums::Mutability;
use sea_orm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Default, Debug, DeriveEntity)]
pub struct Entity;

impl EntityName for Entity {
    fn table_name(&self) -> &str {
        "asset_data"
    }
}

#[derive(Clone, Debug, PartialEq, DeriveModel, DeriveActiveModel, Serialize, Deserialize)]
pub struct Model {
    pub id: i64,
    pub chain_data_mutability: ChainMutability,
    pub schema_version: i32,
    pub chain_data: Json,
    pub metadata_url: String,
    pub metadata_mutability: Mutability,
    pub metadata: Json,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveColumn)]
pub enum Column {
    Id,
    ChainDataMutability,
    SchemaVersion,
    ChainData,
    MetadataUrl,
    MetadataMutability,
    Metadata,
}

#[derive(Copy, Clone, Debug, EnumIter, DerivePrimaryKey)]
pub enum PrimaryKey {
    Id,
}

impl PrimaryKeyTrait for PrimaryKey {
    type ValueType = i64;
    fn auto_increment() -> bool {
        true
    }
}

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {
    Asset,
}

impl ColumnTrait for Column {
    type EntityName = Entity;
    fn def(&self) -> ColumnDef {
        match self {
            Self::Id => ColumnType::BigInteger.def(),
            Self::ChainDataMutability => ChainMutability::db_type(),
            Self::SchemaVersion => ColumnType::Integer.def(),
            Self::ChainData => ColumnType::JsonBinary.def(),
            Self::MetadataUrl => ColumnType::String(Some(200u32)).def(),
            Self::MetadataMutability => Mutability::db_type(),
            Self::Metadata => ColumnType::JsonBinary.def(),
        }
    }
}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        match self {
            Self::Asset => Entity::has_many(super::asset::Entity).into(),
        }
    }
}

impl Related<super::asset::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Asset.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}