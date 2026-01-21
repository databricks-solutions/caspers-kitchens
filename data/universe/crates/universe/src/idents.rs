//! Strongly typed identifiers for use in Casper's Bank simulation.
//!
//! All of these identifiers just wrap a [`Uuid`], and provide a few convenience methods.
//! We define these types to ensure that we don't accidentally mix up IDs of different types.
//!
//! We also try to enforce that IDs are generated in a consistent manner for the specific use case.
//! If we may read data from files, we should not ask users to provide a UUID, but may also
//! require to generate a new simulation object from that can be used to append to existing results.
//!
//! In these cases we require stable ID generation and use UUID v5.
//!
//! Event-like data profits from UUIDs that can be ordered based on time as such we can use
//! UUID v7 for these cases (transactions, transaction items, people).
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
pub struct BranchId(Uuid);

impl BranchId {
    /// Creates a new [`BranchId`] from a URI reference.
    ///
    /// Uri references should be in the form of `branches/<branch_name>`
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        BranchId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(BranchId);

// Legacy alias for backward compatibility in some contexts
pub type SiteId = BranchId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DepartmentId(Uuid);

impl DepartmentId {
    /// Creates a new [`DepartmentId`] from a URI reference.
    ///
    /// Uri references should be in the form of `departments/<department_name>`
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        DepartmentId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(DepartmentId);

// Legacy alias for backward compatibility
pub type KitchenId = DepartmentId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ServicePointId(Uuid);

impl ServicePointId {
    /// Creates a new [`ServicePointId`] from a URI reference.
    ///
    /// Uri references should be in the form of `service_points/<service_point_name>`
    /// Examples: Teller stations, ATMs, Loan officer desks
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        ServicePointId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(ServicePointId);

// Legacy alias for backward compatibility
pub type StationId = ServicePointId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TransactionId(Uuid);

impl Default for TransactionId {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionId {
    /// Creates a new time-ordered [`TransactionId`] using UUID v7.
    pub fn new() -> Self {
        TransactionId(Uuid::now_v7())
    }
}

impl_id_type!(TransactionId);

// Legacy alias for backward compatibility
pub type OrderId = TransactionId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TransactionItemId(Uuid);

impl Default for TransactionItemId {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionItemId {
    /// Creates a new time-ordered [`TransactionItemId`] using UUID v7.
    pub fn new() -> Self {
        TransactionItemId(Uuid::now_v7())
    }
}

impl_id_type!(TransactionItemId);

// Legacy alias for backward compatibility
pub type OrderLineId = TransactionItemId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProductFamilyId(Uuid);

impl ProductFamilyId {
    /// Creates a new [`ProductFamilyId`] from a URI reference.
    ///
    /// Uri references should be in the form of `product_families/<family_name>`
    /// Examples: Credit Cards, Checking Accounts, Mortgages
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        ProductFamilyId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(ProductFamilyId);

// Legacy alias for backward compatibility
pub type BrandId = ProductFamilyId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProductId(Uuid);

impl ProductId {
    /// Creates a new [`ProductId`] from a URI reference.
    ///
    /// Uri references should be in the form of `products/<product_name>`
    /// Examples: Platinum Credit Card, 30-Year Fixed Mortgage, Premium Checking
    pub fn from_uri_ref(name: impl AsRef<str>) -> Self {
        ProductId(Uuid::new_v5(&Uuid::NAMESPACE_URL, name.as_ref().as_bytes()))
    }
}

impl_id_type!(ProductId);

// Legacy alias for backward compatibility
pub type MenuItemId = ProductId;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PersonId(pub(crate) Uuid);

impl Default for PersonId {
    fn default() -> Self {
        Self::new()
    }
}

impl PersonId {
    /// Creates a new time-ordered [`PersonId`] using UUID v7.
    /// Used for both customers and bank officers.
    pub fn new() -> Self {
        PersonId(Uuid::now_v7())
    }
}

impl_id_type!(PersonId);

/// Unique identifier for a bank account (checking, savings, etc.)
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AccountId(Uuid);

impl Default for AccountId {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountId {
    /// Creates a new time-ordered [`AccountId`] using UUID v7.
    pub fn new() -> Self {
        AccountId(Uuid::now_v7())
    }
}

impl_id_type!(AccountId);

/// Unique identifier for a credit/debit card
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CardId(Uuid);

impl Default for CardId {
    fn default() -> Self {
        Self::new()
    }
}

impl CardId {
    /// Creates a new time-ordered [`CardId`] using UUID v7.
    pub fn new() -> Self {
        CardId(Uuid::now_v7())
    }
}

impl_id_type!(CardId);
