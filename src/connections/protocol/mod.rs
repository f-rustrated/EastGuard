//! Wire protocol for client ↔ server traffic.
//!
//! Split into three sub-protocols by audience and routing model:
//!
//! - [`control_plane`] — topic lifecycle and metadata lookups (create / delete /
//!   list / describe). The server resolves the right destination internally;
//!   on the describe path, a non-owner returns a redirect.
//! - [`data_plane`] — produce / fetch / list-offsets. The client routes
//!   directly to the right data node using its local routing cache; on stale
//!   targeting the server returns a redirect error and the client retries.
//! - [`admin`] — operator / debug / integration-test affordances (describe
//!   cluster, force split, shard lookup).
//!
//! All three submodules export their types under `crate::connections::protocol`
//! via glob re-export — call sites import flat names without caring which
//! sub-protocol a type belongs to.

#![allow(dead_code)]

mod admin;
mod control_plane;
mod data_plane;

pub use admin::*;
pub use control_plane::*;
pub use data_plane::*;

use borsh::{BorshDeserialize, BorshSerialize};

use crate::{impl_from_variant, impl_from_variant_via};

// ── Top-level dispatch ─────────────────────────────────────────────────────

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub enum ClientRequest {
    ControlPlane(ControlPlaneRequest),
    DataPlane(ClientDataPlaneRequest),
    Admin(AdminRequest),
}

#[derive(Debug, BorshSerialize, BorshDeserialize)]
pub enum ClientResponse {
    ControlPlane(ControlPlaneResponse),
    DataPlane(DataPlaneResponse),
    Admin(AdminResponse),
    Stop,
}

impl_from_variant!(
    ClientRequest,
    ControlPlane(ControlPlaneRequest),
    DataPlane(ClientDataPlaneRequest),
    Admin(AdminRequest),
);

impl_from_variant!(
    ClientResponse,
    ControlPlane(ControlPlaneResponse),
    DataPlane(DataPlaneResponse),
    Admin(AdminResponse),
);

impl_from_variant_via!(
    ClientRequest,
    ClientDataPlaneRequest,
    ProduceRequest,
    FetchRequest,
    FetchByIdRequest,
    ListOffsetsRequest
);
