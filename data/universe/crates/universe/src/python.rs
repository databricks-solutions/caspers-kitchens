use itertools::Itertools as _;
use pyo3::prelude::*;

use crate::{
    Brand, Ingredient, IngredientQuantity, Instruction, Kitchen, KitchenSetup, MenuItem,
    SimulationSetup, Site, SiteSetup, Station,
};

#[pymethods]
impl SimulationSetup {
    fn __repr__(&self) -> String {
        let sites = self
            .sites
            .iter()
            .map(|s| s.__repr__())
            .collect_vec()
            .join(", ");
        let brands = self
            .brands
            .iter()
            .map(|b| b.__repr__())
            .collect_vec()
            .join(", ");
        format!("SimulationSetup(sites=[{}], brands=[{}])", sites, brands)
    }
}

#[pymethods]
impl SiteSetup {
    fn __repr__(&self) -> String {
        let info = self
            .info
            .as_ref()
            .map_or("None".to_string(), |i| i.__repr__());
        let kitchens = self
            .kitchens
            .iter()
            .map(|s| s.__repr__())
            .collect_vec()
            .join(", ");
        format!("SiteSetup(info={}, kitchens=[{}])", info, kitchens)
    }
}

#[pymethods]
impl Site {
    #[new]
    #[pyo3(signature = (id, name, latitude, longitude))]
    fn new(id: String, name: String, latitude: f64, longitude: f64) -> Self {
        Site {
            id,
            name,
            latitude,
            longitude,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Site(id={}, name={}, latitude={}, longitude={})",
            self.id, self.name, self.latitude, self.longitude
        )
    }
}

#[pymethods]
impl Kitchen {
    fn __repr__(&self) -> String {
        format!("Kitchen(id={}, name={}", self.id, self.name)
    }
}

#[pymethods]
impl KitchenSetup {
    fn __repr__(&self) -> String {
        let info = self
            .info
            .as_ref()
            .map_or("None".to_string(), |i| i.__repr__());
        let stations = self
            .stations
            .iter()
            .map(|s| s.__repr__())
            .collect_vec()
            .join(", ");
        format!("KitchenSetup(info={info}, stations=[{}])", stations)
    }
}

#[pymethods]
impl Station {
    fn __repr__(&self) -> String {
        format!(
            "Station(id={}, name={}, station_type={:?})",
            self.id, self.name, self.station_type
        )
    }
}

#[pymethods]
impl Brand {
    fn __repr__(&self) -> String {
        format!(
            "Brand(id={}, name={}, description={}, category={}, items=[{}])",
            self.id,
            self.name,
            self.description,
            self.category,
            self.items
                .iter()
                .map(|i| i.__repr__())
                .collect_vec()
                .join(", ")
        )
    }
}

#[pymethods]
impl MenuItem {
    fn __repr__(&self) -> String {
        format!(
            "MenuItem(id={}, name={}, description={}, price={}, image_url={}, ingredients=[{}], instructions=[{}])",
            self.id,
            self.name,
            self.description,
            self.price,
            self.image_url.as_ref().unwrap_or(&"None".to_string()),
            self.ingredients
                .iter()
                .map(|i| i.__repr__())
                .collect_vec()
                .join(", "),
            self.instructions
                .iter()
                .map(|i| i.__repr__())
                .collect_vec()
                .join(", ")
        )
    }
}

#[pymethods]
impl Ingredient {
    fn __repr__(&self) -> String {
        format!(
            "Ingredient(id={}, name={}, description={}, price={}, image_url={})",
            self.id,
            self.name,
            self.description,
            self.price,
            self.image_url.as_ref().unwrap_or(&"None".to_string())
        )
    }
}

#[pymethods]
impl IngredientQuantity {
    fn __repr__(&self) -> String {
        format!(
            "IngredientQuantity(ingredient_ref={}, quantity={})",
            self.ingredient_ref, self.quantity
        )
    }
}

#[pymethods]
impl Instruction {
    fn __repr__(&self) -> String {
        format!(
            "Instruction(step={}, description={}, required_station={}, expected_duration={})",
            self.step, self.description, self.required_station, self.expected_duration
        )
    }
}
