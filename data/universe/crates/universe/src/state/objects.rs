use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::array::cast::AsArray as _;
use dashmap::DashMap;
use dashmap::mapref::one::Ref;
use indexmap::IndexMap;
use itertools::Itertools as _;
use rand::Rng as _;
use strum::AsRefStr;

use crate::Error;
use crate::error::Result;
use crate::idents::{BrandId, KitchenId, MenuItemId, SiteId, StationId};
use crate::models::{MenuItem, Site, Station};

use super::EntityView;

use crate::builders::ObjectDataBuilder;

#[derive(Debug, thiserror::Error)]
enum VendorDataError {
    #[error("Not found")]
    NotFound,

    #[error("Inconsistent data")]
    InconsistentData,

    #[error("Column not found")]
    ColumnNotFound(&'static str),
}

impl From<VendorDataError> for Error {
    fn from(err: VendorDataError) -> Self {
        match err {
            VendorDataError::NotFound => Error::NotFound,
            VendorDataError::InconsistentData => Error::InvalidData(err.to_string()),
            VendorDataError::ColumnNotFound(_) => Error::InvalidData(err.to_string()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, AsRefStr)]
#[strum(serialize_all = "snake_case")]
pub enum ObjectLabel {
    Site,
    Kitchen,
    Station,
    Brand,
    MenuItem,
}

pub struct ObjectData {
    objects: RecordBatch,

    // kitchen_slices: HashMap<KitchenId, (usize, usize)>,
    menu_items: Arc<DashMap<MenuItemId, MenuItem>>,

    menu_item_idx: IndexMap<MenuItemId, usize>,
}

impl ObjectData {
    pub fn builder() -> ObjectDataBuilder {
        ObjectDataBuilder::new()
    }

    /// Record batch MUST be sorted by parent_id.
    pub fn try_new(objects: RecordBatch) -> Result<Self> {
        let data = Self {
            objects,
            menu_items: Arc::new(DashMap::new()),
            menu_item_idx: Default::default(),
        };
        data.update_indices()
    }

    fn update_indices(mut self) -> Result<Self> {
        let menu_item_idx = self
            .iter_ids()?
            .enumerate()
            .filter(|&(_, (_, _, label))| label == Some(ObjectLabel::MenuItem.as_ref()))
            .filter_map(|(idx, (id, _parent_id, _))| {
                id.and_then(|id| Some((id.try_into().ok()?, idx)))
            })
            .collect();
        self.menu_item_idx = menu_item_idx;
        Ok(self)
    }

    pub(crate) fn objects(&self) -> &RecordBatch {
        &self.objects
    }

    #[allow(clippy::type_complexity)]
    fn iter_ids(
        &self,
    ) -> Result<impl Iterator<Item = (Option<&[u8]>, Option<&[u8]>, Option<&str>)>> {
        let ids = self
            .objects
            .column_by_name("id")
            .ok_or(VendorDataError::ColumnNotFound("id"))?
            .as_fixed_size_binary();

        let parent_ids = self
            .objects
            .column_by_name("parent_id")
            .ok_or(VendorDataError::ColumnNotFound("parent_id"))?
            .as_fixed_size_binary();

        let labels = self
            .objects
            .column_by_name("label")
            .ok_or(VendorDataError::ColumnNotFound("label"))?
            .as_string_view();

        Ok(ids
            .iter()
            .zip(parent_ids.iter())
            .zip(labels.iter())
            .map(|((id, parent_id), label)| (id, parent_id, label)))
    }

    /// Get the parsed properties for a menu item
    pub(crate) fn menu_item(&self, item_id: &MenuItemId) -> Result<Ref<'_, MenuItemId, MenuItem>> {
        if let Some(item) = self.menu_items.get(item_id) {
            return Ok(item);
        }
        let view = self
            .menu_item_data(item_id)
            .ok_or(VendorDataError::NotFound)?;
        let properties = view.properties()?;
        self.menu_items.insert(*item_id, properties.clone());
        Ok(self.menu_items.get(item_id).unwrap())
    }

    pub(crate) fn menu_item_data(&self, item_id: &MenuItemId) -> Option<MenuItemView<'_>> {
        let (id, idx) = self.menu_item_idx.get_key_value(item_id)?;
        Some(MenuItemView::new(id, self, *idx))
    }

    pub fn sample_menu_items(
        &self,
        count: Option<usize>,
        rng: &mut rand::rngs::ThreadRng,
    ) -> Vec<MenuItemView<'_>> {
        let count = count.unwrap_or_else(|| rng.random_range(1..6));
        let mut selected_items = Vec::with_capacity(count);
        for _ in 0..count {
            let item_index = rng.random_range(0..self.menu_item_idx.len());
            let (id, idx) = self.menu_item_idx.get_index(item_index).unwrap();
            selected_items.push(MenuItemView::new(id, self, *idx));
        }
        selected_items
    }

    pub fn sites(&self) -> Result<impl Iterator<Item = SiteView<'_>>> {
        Ok(self
            .iter_ids()?
            .enumerate()
            .filter_map(|(index, (id, _parent_id, label))| {
                id.and_then(|_| {
                    (label == Some(ObjectLabel::Site.as_ref())).then_some(SiteView {
                        data: self,
                        valid_index: index,
                    })
                })
            }))
    }

    pub(crate) fn site(&self, site_id: &SiteId) -> Result<SiteView<'_>> {
        self.iter_ids()?
            .enumerate()
            .find(|(_, (id, _, label))| {
                *label == Some(ObjectLabel::Site.as_ref()) && *id == Some(site_id.as_ref())
            })
            .map(|(index, _)| SiteView {
                data: self,
                valid_index: index,
            })
            .ok_or(VendorDataError::NotFound.into())
    }

    pub(crate) fn kitchens(
        &self,
        site_id: &SiteId,
    ) -> Result<impl Iterator<Item = Result<(KitchenId, Vec<BrandId>)>>> {
        let brands: Vec<_> = self
            .iter_ids()?
            .filter(|&(id, _, label)| label == Some(ObjectLabel::Brand.as_ref()) && id.is_some())
            .map(|(id, _, _)| uuid::Uuid::from_slice(id.unwrap()).map(|id| id.into()))
            .try_collect()?;
        Ok(self.iter_ids()?.filter_map(move |(id, parent_id, label)| {
            id.and_then(|id| {
                (parent_id == Some(site_id.as_ref())
                    && label == Some(ObjectLabel::Kitchen.as_ref()))
                .then(|| Ok((uuid::Uuid::from_slice(id)?.into(), brands.clone())))
            })
        }))
    }

    pub(crate) fn kitchen_stations(
        &self,
        kitchen_id: &KitchenId,
    ) -> Result<impl Iterator<Item = Result<(StationId, Station)>>> {
        let properties = self
            .objects
            .column_by_name("properties")
            .ok_or(VendorDataError::ColumnNotFound("properties"))?
            .as_string::<i64>();
        Ok(self.iter_ids()?.zip(properties.iter()).filter_map(
            |((id, parent_id, label), properties)| {
                id.and_then(|id| {
                    (parent_id == Some(kitchen_id.as_ref())
                        && label == Some(ObjectLabel::Station.as_ref()))
                    .then(|| {
                        Ok((
                            uuid::Uuid::from_slice(id)?.into(),
                            serde_json::from_str(
                                properties.ok_or(VendorDataError::InconsistentData)?,
                            )?,
                        ))
                    })
                })
            },
        ))
    }
}

pub struct MenuItemView<'a> {
    id: &'a MenuItemId,

    /// Reference to global object data
    data: &'a ObjectData,

    /// Index of the valid row in the data for the given site.
    valid_index: usize,
}

impl EntityView for MenuItemView<'_> {
    type Id = MenuItemId;
    type Properties = MenuItem;

    fn id(&self) -> Self::Id {
        *self.id
    }

    fn data(&self) -> &ObjectData {
        self.data
    }

    fn valid_index(&self) -> usize {
        self.valid_index
    }
}

impl<'a> MenuItemView<'a> {
    pub fn new(id: &'a MenuItemId, data: &'a ObjectData, valid_index: usize) -> Self {
        Self {
            id,
            data,
            valid_index,
        }
    }

    pub fn brand_id(&self) -> &[u8] {
        self.data
            .objects()
            .column_by_name("parent_id")
            .unwrap()
            .as_fixed_size_binary()
            .value(self.valid_index)
    }
}

pub struct SiteView<'a> {
    /// Reference to global object data
    data: &'a ObjectData,

    /// Index of the valid row in the data for the given site.
    valid_index: usize,
}

impl EntityView for SiteView<'_> {
    type Id = SiteId;
    type Properties = Site;

    fn data(&self) -> &ObjectData {
        self.data
    }

    fn valid_index(&self) -> usize {
        self.valid_index
    }
}
