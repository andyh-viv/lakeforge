//! SQL front-end for Forge: session construction, catalog/table registration,
//! Delta Lake integration and object-store wiring shared by driver and executors.

pub mod delta_provider;
pub mod managed;
pub mod object_store;
pub mod session;
pub mod tables;

pub use managed::{create_managed_table, drop_managed_table, managed_location, CreateOutcome};
pub use session::{ForgeSessionBuilder, ForgeSessionExt};
pub use tables::{register_table, TableFormat, TableSpec};
