use std::sync::Arc;

use crate::error::Result;

pub use caspers::models::v1::*;

pub type MenuItemRef = Arc<MenuItem>;

pub mod caspers {
    pub mod models {
        pub mod v1 {
            include!("./gen/caspers.core.v1.rs");
        }
    }
    pub mod messages {
        pub mod v1 {
            include!("./gen/caspers.messages.v1.rs");
        }
    }

    pub mod vendors {
        pub mod v1 {
            include!("./gen/caspers.vendors.v1.rs");
        }
    }
}

impl Site {
    pub fn lat_lng(&self) -> Result<h3o::LatLng> {
        h3o::LatLng::new(self.latitude, self.longitude)
            .map_err(|e| crate::error::Error::InvalidGeometry(e.to_string()))
    }
}
