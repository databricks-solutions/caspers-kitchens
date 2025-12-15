use arrow::array::RecordBatch;
use datafusion::common::HashMap;
use futures::TryStreamExt;
use itertools::Itertools as _;
use object_store::ObjectStore;
use object_store::path::Path;
use tracing::instrument;
use url::Url;
use uuid::Uuid;

pub use self::builders::*;
pub use self::error::*;
pub use self::idents::*;
pub use self::models::*;
pub use self::simulation::*;
pub use self::state::*;
#[cfg(any(test, feature = "templates"))]
pub use self::templates::*;
pub use crate::context::*;

#[cfg(feature = "python")]
use pyo3::prelude::*;

mod agents;
mod builders;
mod context;
mod error;
mod functions;
mod idents;
mod models;
#[cfg(feature = "python")]
mod python;
mod simulation;
mod state;
#[cfg(any(test, feature = "templates"))]
mod templates;
#[cfg(any(test, feature = "templates"))]
pub mod test_utils;

#[cfg_attr(feature = "python", pyclass(get_all, set_all))]
#[derive(Debug, Clone)]
pub struct SimulationSetup {
    pub sites: Vec<SiteSetup>,
    pub brands: Vec<Brand>,
}

impl SimulationSetup {
    pub async fn load(store: &dyn ObjectStore, base_path: &Path) -> Result<Self> {
        let sites_path = base_path.child("sites");
        let brands_path = base_path.child("brands");

        let sites = SimulationSetup::load_sites(store, &sites_path).await?;
        let brands = SimulationSetup::load_brands(store, &brands_path).await?;

        Ok(SimulationSetup { sites, brands })
    }

    async fn load_sites(store: &dyn ObjectStore, sites_path: &Path) -> Result<Vec<SiteSetup>> {
        let site_files: Vec<_> = store.list(Some(sites_path)).try_collect().await?;

        let mut sites = Vec::new();
        for file in site_files
            .into_iter()
            .filter(|file| file.location.extension() == Some("json"))
        {
            let site_bytes = store.get(&file.location).await?.bytes().await?;
            let mut site_setup: SiteSetup = serde_json::from_slice(&site_bytes)?;
            if let Some(ref mut site) = site_setup.info {
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

                sites.push(site_setup);
            } else {
                return Err(Error::invalid_data("missing site information"));
            };
        }

        Ok(sites)
    }

    async fn load_brands(store: &dyn ObjectStore, brands_path: &Path) -> Result<Vec<Brand>> {
        let brand_files: Vec<_> = store.list(Some(brands_path)).try_collect().await?;
        let mut brands = Vec::new();

        for file in brand_files {
            let brand_data = store.get(&file.location).await?.bytes().await?;
            let mut brand: Brand = serde_json::from_slice(&brand_data)?;
            brand.id = BrandId::from_uri_ref(format!("brands/{}", brand.name)).to_string();

            for menu_item in brand.items.iter_mut() {
                menu_item.id = MenuItemId::from_uri_ref(format!(
                    "brands/{}/menu_items/{}",
                    brand.id, menu_item.name
                ))
                .to_string();
            }

            brands.push(brand);
        }

        Ok(brands)
    }

    pub fn object_data(&self) -> Result<RecordBatch> {
        let brands: HashMap<_, _> = self
            .brands
            .iter()
            .map(|brand| Ok::<_, Error>((Uuid::parse_str(&brand.id)?.into(), brand)))
            .try_collect()?;
        generate_objects(&brands, &self.sites)
    }
}

fn generate_objects(brands: &HashMap<BrandId, &Brand>, sites: &[SiteSetup]) -> Result<RecordBatch> {
    let mut builder = ObjectDataBuilder::new();

    for (brand_id, brand) in brands.iter() {
        builder.append_brand(brand_id, brand);
    }

    for site in sites {
        builder.append_site_info(site)?;
    }

    builder.finish()
}

pub async fn load_simulation_setup<I, K, V>(url: &Url, options: I) -> Result<SimulationSetup>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let (store, path) = object_store::parse_url_opts(url, options)?;
    SimulationSetup::load(&store, &path).await
}

#[instrument(name = "run_simulation", skip_all)]
pub async fn run_simulation(
    duration: usize,
    working_directory: Url,
    dry_run: bool,
) -> Result<(), Error> {
    let mut simulation = SimulationBuilder::new()
        .with_working_directory(working_directory)
        .with_dry_run(dry_run)
        .build()
        .await?;
    simulation.run(duration).await?;
    Ok(())
}

pub fn resolve_url(path: Option<impl AsRef<str>>) -> Result<url::Url> {
    match path {
        Some(path) => match url::Url::parse(path.as_ref()) {
            Ok(url) => Ok(url),
            Err(_) => {
                let path = std::fs::canonicalize(path.as_ref())?;
                Ok(url::Url::from_directory_path(path).unwrap())
            }
        },
        None => {
            Ok(url::Url::from_directory_path(std::env::current_dir()?.join(".caspers/")).unwrap())
        }
    }
}
