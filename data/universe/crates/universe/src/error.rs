pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Not found")]
    NotFound,

    #[error("Missing input: {0}")]
    MissingInput(String),

    #[error("Invalid data: {0}")]
    InvalidData(String),

    #[error("Missing geometry")]
    MissingGeometry,

    #[error("Invalid geometry: {0}")]
    InvalidGeometry(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Generic error: {0}")]
    Generic(Box<dyn std::error::Error>),

    #[error("Invalid uuid")]
    InvalidUuid {
        #[from]
        source: uuid::Error,
    },

    #[error("Invalid url")]
    InvalidUrl {
        #[from]
        source: url::ParseError,
    },

    #[error("Arrow error: {source}")]
    Arrow {
        #[from]
        source: arrow_schema::ArrowError,
    },

    #[error("GeoArrow error: {source}")]
    GeoArrow {
        #[from]
        source: geoarrow_schema::error::GeoArrowError,
    },

    #[error("Datafusion error: {source}")]
    Datafusion {
        #[from]
        source: datafusion::common::DataFusionError,
    },

    #[error("Serde error: {source}")]
    Serde {
        #[from]
        source: serde_json::Error,
    },

    #[error("Parquet error: {source}")]
    Parquet {
        #[from]
        source: parquet::errors::ParquetError,
    },

    #[error("H3 error: {source}")]
    H3 { source: Box<dyn std::error::Error> },

    #[error("Rand error: {source}")]
    Rand {
        #[from]
        source: rand::distr::uniform::Error,
    },

    #[error("ObjectStore error: {source}")]
    ObjectStore {
        #[from]
        source: object_store::Error,
    },

    #[error("Io error: {source}")]
    Url {
        #[from]
        source: std::io::Error,
    },
}

impl From<h3o::error::DissolutionError> for Error {
    fn from(error: h3o::error::DissolutionError) -> Self {
        Error::H3 {
            source: Box::new(error),
        }
    }
}

impl From<h3o::error::InvalidCellIndex> for Error {
    fn from(error: h3o::error::InvalidCellIndex) -> Self {
        Error::H3 {
            source: Box::new(error),
        }
    }
}

impl From<h3o::error::InvalidLatLng> for Error {
    fn from(error: h3o::error::InvalidLatLng) -> Self {
        Error::H3 {
            source: Box::new(error),
        }
    }
}

impl Error {
    pub fn invalid_data(message: impl ToString) -> Self {
        Error::InvalidData(message.to_string())
    }

    pub fn invalid_geometry(message: impl ToString) -> Self {
        Error::InvalidGeometry(message.to_string())
    }

    pub fn generic(error: impl std::error::Error + 'static) -> Self {
        Error::Generic(Box::new(error))
    }

    pub fn internal(message: impl ToString) -> Self {
        Error::InternalError(message.to_string())
    }
}
