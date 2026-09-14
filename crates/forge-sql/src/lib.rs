//! SQL front-end for Forge: session construction, catalog/table registration,
//! Delta Lake integration and object-store wiring shared by driver and executors.

pub mod object_store;
pub mod session;
pub mod tables;

pub use session::{ForgeSessionBuilder, ForgeSessionExt};
pub use tables::{register_table, TableFormat, TableSpec};
