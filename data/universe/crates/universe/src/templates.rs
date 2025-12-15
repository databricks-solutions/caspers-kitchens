use crate::{
    Brand, BrandId, EntityView, Error, KitchenId, MenuItemId, ObjectData, PopulationData,
    SimulationContext, SimulationSetup, SiteId, SiteSetup, StationId,
};
use itertools::Itertools as _;
use rand::Rng as _;

use crate::error::Result;

pub async fn initialize_template(caspers_directory: &url::Url, template: Template) -> Result<()> {
    let setup = template.load()?;
    let objects = setup.object_data()?;
    let object_data = ObjectData::try_new(objects)?;

    let mut builder = PopulationData::builder();
    for site in object_data.sites()? {
        let n_people = rand::rng().random_range(500..1500);
        let info = site.properties()?;
        builder.add_site(n_people, info.latitude, info.longitude)?;
    }
    let population_data = builder.finish()?;

    let _ctx = SimulationContext::builder()
        .with_working_directory(caspers_directory.clone())
        .with_object_data(object_data)
        .with_population_data(population_data)
        .build()
        .await?;

    Ok(())
}

pub struct Template {
    sites: Vec<SiteTemplate>,
    brands: Vec<BrandTemplate>,
}

impl Default for Template {
    fn default() -> Self {
        Self {
            sites: vec![SiteTemplate::Amsterdam, SiteTemplate::London],
            brands: vec![
                BrandTemplate::Asian,
                BrandTemplate::FastFood,
                BrandTemplate::Mexican,
            ],
        }
    }
}

impl Template {
    pub fn new(sites: Vec<SiteTemplate>, brands: Vec<BrandTemplate>) -> Self {
        Self { sites, brands }
    }

    pub fn load(&self) -> Result<SimulationSetup> {
        load_template(self)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum SiteTemplate {
    Amsterdam,
    Berlin,
    London,
}

impl std::fmt::Display for SiteTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SiteTemplate::Amsterdam => write!(f, "Amsterdam"),
            SiteTemplate::Berlin => write!(f, "Berlin"),
            SiteTemplate::London => write!(f, "London"),
        }
    }
}

impl SiteTemplate {
    fn data(&self) -> &[u8] {
        match self {
            SiteTemplate::Amsterdam => include_bytes!("../templates/base/sites/amsterdam.json"),
            SiteTemplate::Berlin => todo!(),
            SiteTemplate::London => include_bytes!("../templates/base/sites/london.json"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BrandTemplate {
    Asian,
    FastFood,
    Mexican,
}

impl std::fmt::Display for BrandTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrandTemplate::Asian => write!(f, "Asian"),
            BrandTemplate::FastFood => write!(f, "FastFood"),
            BrandTemplate::Mexican => write!(f, "Mexican"),
        }
    }
}

impl BrandTemplate {
    pub fn data(&self) -> &[u8] {
        match self {
            BrandTemplate::Asian => include_bytes!("../templates/base/brands/asian.json"),
            BrandTemplate::FastFood => include_bytes!("../templates/base/brands/fast_food.json"),
            BrandTemplate::Mexican => include_bytes!("../templates/base/brands/mexican.json"),
        }
    }

    pub fn brand(&self) -> Result<Brand> {
        load_brand(self)
    }
}

fn load_template(template: &Template) -> Result<SimulationSetup> {
    let sites = template.sites.iter().map(load_site).try_collect()?;
    let brands = template.brands.iter().map(load_brand).try_collect()?;
    Ok(SimulationSetup { sites, brands })
}

fn load_brand(brand: &BrandTemplate) -> Result<Brand> {
    let mut brand: Brand = serde_json::from_slice(brand.data()).map_err(Error::from)?;
    brand.id = BrandId::from_uri_ref(format!("brands/{}", brand.name)).to_string();

    for menu_item in brand.items.iter_mut() {
        menu_item.id =
            MenuItemId::from_uri_ref(format!("brands/{}/menu_items/{}", brand.id, menu_item.name))
                .to_string();
    }

    Ok(brand)
}

fn load_site(site: &SiteTemplate) -> Result<SiteSetup> {
    let mut site_setup: SiteSetup = serde_json::from_slice(site.data()).map_err(Error::from)?;
    let Some(ref mut site) = site_setup.info else {
        return Err(Error::invalid_data("missing site information"));
    };
    site.id = SiteId::from_uri_ref(format!("sites/{}", site.name)).to_string();
    site_setup.kitchens = site_setup
        .kitchens
        .into_iter()
        .map(|mut kitchen_setup| {
            if let Some(ref mut kitchen) = kitchen_setup.info {
                kitchen.id = KitchenId::from_uri_ref(format!(
                    "sites/{}/kitchens/{}",
                    site.name, kitchen.name
                ))
                .to_string();

                for station in &mut kitchen_setup.stations {
                    station.id = StationId::from_uri_ref(format!(
                        "sites/{}/kitchens/{}/stations/{}",
                        site.name, kitchen.name, station.name
                    ))
                    .to_string();
                }
            }

            kitchen_setup
        })
        .collect();

    Ok(site_setup)
}
