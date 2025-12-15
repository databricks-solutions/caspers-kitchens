//! Strongly types identifiers for use in the universe.
//!
//! All of these identifiers just wrap a [`Uuid`], and provide a few convenience methods.
//! We define these types to ensure that we don't accidentally mix up IDs of different types.
//!
//! We also try to enforce that IDs are generated in a consistent manner for the specific use case.
//! If we may read data from files, we should not ask users to provide a UUID, but may also
//! require to generate a new simulation object from that can be used to append to existing results.
//!
//! In these cases we require stabe ID generation and use UUID v5.
//!
//! Event-like data profits from UUIDs that can be ordered based on time as such we can use
//! UUID v7 for these cases.
//!
//! [`Uuid`]: uuid::Uuid
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::Error;

pub trait TypedId:
    Clone + PartialEq + Eq + AsRef<Uuid> + AsRef<[u8]> + ToString + From<Uuid>
{
}

/// Implements common traits for Uuid-based ID types
macro_rules! impl_id_type {
    ($type:ident) => {
        impl TypedId for $type {}

        impl From<Uuid> for $type {
            fn from(id: Uuid) -> Self {
                $type(id)
            }
        }

        impl TryFrom<&[u8]> for $type {
            type Error = Error;

            fn try_from(id: &[u8]) -> Result<Self, Self::Error> {
                Ok($type(Uuid::from_slice(id).map_err(|e| Error::generic(e))?))
            }
        }

        impl AsRef<Uuid> for $type {
            fn as_ref(&self) -> &Uuid {
                &self.0
            }
        }

        impl AsRef<[u8]> for $type {
            fn as_ref(&self) -> &[u8] {
                self.0.as_bytes()
            }
        }

        impl std::fmt::Display for $type {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SiteId(Uuid);

impl SiteId {
    /// Creates a new [`SiteId`] from a URI reference.
    ///
    /// Uri references should be in the form of `sites/<site_name>`
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        SiteId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(SiteId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KitchenId(Uuid);

impl KitchenId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        KitchenId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(KitchenId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct StationId(Uuid);

impl StationId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        StationId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(StationId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderId(Uuid);

impl Default for OrderId {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderId {
    pub fn new() -> Self {
        OrderId(Uuid::now_v7())
    }
}

impl_id_type!(OrderId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OrderLineId(Uuid);

impl Default for OrderLineId {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderLineId {
    pub fn new() -> Self {
        OrderLineId(Uuid::now_v7())
    }
}

impl_id_type!(OrderLineId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct BrandId(Uuid);

impl BrandId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        BrandId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(BrandId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MenuItemId(Uuid);

impl MenuItemId {
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        MenuItemId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(MenuItemId);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PersonId(pub(crate) Uuid);

impl Default for PersonId {
    fn default() -> Self {
        Self::new()
    }
}

impl PersonId {
    pub fn new() -> Self {
        PersonId(Uuid::now_v7())
    }
}

impl_id_type!(PersonId);
