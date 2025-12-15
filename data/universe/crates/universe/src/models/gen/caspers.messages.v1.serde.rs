// @generated
impl serde::Serialize for CloudEvent {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if !self.source.is_empty() {
            len += 1;
        }
        if !self.spec_version.is_empty() {
            len += 1;
        }
        if !self.r#type.is_empty() {
            len += 1;
        }
        if !self.attributes.is_empty() {
            len += 1;
        }
        if self.time.is_some() {
            len += 1;
        }
        if self.data.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("caspers.messages.v1.CloudEvent", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if !self.source.is_empty() {
            struct_ser.serialize_field("source", &self.source)?;
        }
        if !self.spec_version.is_empty() {
            struct_ser.serialize_field("spec_version", &self.spec_version)?;
        }
        if !self.r#type.is_empty() {
            struct_ser.serialize_field("type", &self.r#type)?;
        }
        if !self.attributes.is_empty() {
            struct_ser.serialize_field("attributes", &self.attributes)?;
        }
        if let Some(v) = self.time.as_ref() {
            struct_ser.serialize_field("time", v)?;
        }
        if let Some(v) = self.data.as_ref() {
            match v {
                cloud_event::Data::BinaryData(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("binary_data", pbjson::private::base64::encode(&v).as_str())?;
                }
                cloud_event::Data::TextData(v) => {
                    struct_ser.serialize_field("text_data", v)?;
                }
                cloud_event::Data::ProtoData(v) => {
                    struct_ser.serialize_field("proto_data", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CloudEvent {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "source",
            "spec_version",
            "specVersion",
            "type",
            "attributes",
            "time",
            "binary_data",
            "binaryData",
            "text_data",
            "textData",
            "proto_data",
            "protoData",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Source,
            SpecVersion,
            Type,
            Attributes,
            Time,
            BinaryData,
            TextData,
            ProtoData,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "source" => Ok(GeneratedField::Source),
                            "specVersion" | "spec_version" => Ok(GeneratedField::SpecVersion),
                            "type" => Ok(GeneratedField::Type),
                            "attributes" => Ok(GeneratedField::Attributes),
                            "time" => Ok(GeneratedField::Time),
                            "binaryData" | "binary_data" => Ok(GeneratedField::BinaryData),
                            "textData" | "text_data" => Ok(GeneratedField::TextData),
                            "protoData" | "proto_data" => Ok(GeneratedField::ProtoData),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CloudEvent;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct caspers.messages.v1.CloudEvent")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CloudEvent, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut source__ = None;
                let mut spec_version__ = None;
                let mut r#type__ = None;
                let mut attributes__ = None;
                let mut time__ = None;
                let mut data__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Source => {
                            if source__.is_some() {
                                return Err(serde::de::Error::duplicate_field("source"));
                            }
                            source__ = Some(map_.next_value()?);
                        }
                        GeneratedField::SpecVersion => {
                            if spec_version__.is_some() {
                                return Err(serde::de::Error::duplicate_field("specVersion"));
                            }
                            spec_version__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Type => {
                            if r#type__.is_some() {
                                return Err(serde::de::Error::duplicate_field("type"));
                            }
                            r#type__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Attributes => {
                            if attributes__.is_some() {
                                return Err(serde::de::Error::duplicate_field("attributes"));
                            }
                            attributes__ = Some(
                                map_.next_value::<std::collections::HashMap<_, _>>()?
                            );
                        }
                        GeneratedField::Time => {
                            if time__.is_some() {
                                return Err(serde::de::Error::duplicate_field("time"));
                            }
                            time__ = map_.next_value()?;
                        }
                        GeneratedField::BinaryData => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("binaryData"));
                            }
                            data__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| cloud_event::Data::BinaryData(x.0));
                        }
                        GeneratedField::TextData => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("textData"));
                            }
                            data__ = map_.next_value::<::std::option::Option<_>>()?.map(cloud_event::Data::TextData);
                        }
                        GeneratedField::ProtoData => {
                            if data__.is_some() {
                                return Err(serde::de::Error::duplicate_field("protoData"));
                            }
                            data__ = map_.next_value::<::std::option::Option<_>>()?.map(cloud_event::Data::ProtoData)
;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CloudEvent {
                    id: id__.unwrap_or_default(),
                    source: source__.unwrap_or_default(),
                    spec_version: spec_version__.unwrap_or_default(),
                    r#type: r#type__.unwrap_or_default(),
                    attributes: attributes__.unwrap_or_default(),
                    time: time__,
                    data: data__,
                })
            }
        }
        deserializer.deserialize_struct("caspers.messages.v1.CloudEvent", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for cloud_event::CloudEventAttributeValue {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if self.attr.is_some() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("caspers.messages.v1.CloudEvent.CloudEventAttributeValue", len)?;
        if let Some(v) = self.attr.as_ref() {
            match v {
                cloud_event::cloud_event_attribute_value::Attr::CeBoolean(v) => {
                    struct_ser.serialize_field("ce_boolean", v)?;
                }
                cloud_event::cloud_event_attribute_value::Attr::CeInteger(v) => {
                    struct_ser.serialize_field("ce_integer", v)?;
                }
                cloud_event::cloud_event_attribute_value::Attr::CeString(v) => {
                    struct_ser.serialize_field("ce_string", v)?;
                }
                cloud_event::cloud_event_attribute_value::Attr::CeBytes(v) => {
                    #[allow(clippy::needless_borrow)]
                    #[allow(clippy::needless_borrows_for_generic_args)]
                    struct_ser.serialize_field("ce_bytes", pbjson::private::base64::encode(&v).as_str())?;
                }
                cloud_event::cloud_event_attribute_value::Attr::CeUri(v) => {
                    struct_ser.serialize_field("ce_uri", v)?;
                }
                cloud_event::cloud_event_attribute_value::Attr::CeUriRef(v) => {
                    struct_ser.serialize_field("ce_uri_ref", v)?;
                }
                cloud_event::cloud_event_attribute_value::Attr::CeTimestamp(v) => {
                    struct_ser.serialize_field("ce_timestamp", v)?;
                }
            }
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for cloud_event::CloudEventAttributeValue {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "ce_boolean",
            "ceBoolean",
            "ce_integer",
            "ceInteger",
            "ce_string",
            "ceString",
            "ce_bytes",
            "ceBytes",
            "ce_uri",
            "ceUri",
            "ce_uri_ref",
            "ceUriRef",
            "ce_timestamp",
            "ceTimestamp",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            CeBoolean,
            CeInteger,
            CeString,
            CeBytes,
            CeUri,
            CeUriRef,
            CeTimestamp,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "ceBoolean" | "ce_boolean" => Ok(GeneratedField::CeBoolean),
                            "ceInteger" | "ce_integer" => Ok(GeneratedField::CeInteger),
                            "ceString" | "ce_string" => Ok(GeneratedField::CeString),
                            "ceBytes" | "ce_bytes" => Ok(GeneratedField::CeBytes),
                            "ceUri" | "ce_uri" => Ok(GeneratedField::CeUri),
                            "ceUriRef" | "ce_uri_ref" => Ok(GeneratedField::CeUriRef),
                            "ceTimestamp" | "ce_timestamp" => Ok(GeneratedField::CeTimestamp),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = cloud_event::CloudEventAttributeValue;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct caspers.messages.v1.CloudEvent.CloudEventAttributeValue")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<cloud_event::CloudEventAttributeValue, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut attr__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::CeBoolean => {
                            if attr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ceBoolean"));
                            }
                            attr__ = map_.next_value::<::std::option::Option<_>>()?.map(cloud_event::cloud_event_attribute_value::Attr::CeBoolean);
                        }
                        GeneratedField::CeInteger => {
                            if attr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ceInteger"));
                            }
                            attr__ = map_.next_value::<::std::option::Option<::pbjson::private::NumberDeserialize<_>>>()?.map(|x| cloud_event::cloud_event_attribute_value::Attr::CeInteger(x.0));
                        }
                        GeneratedField::CeString => {
                            if attr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ceString"));
                            }
                            attr__ = map_.next_value::<::std::option::Option<_>>()?.map(cloud_event::cloud_event_attribute_value::Attr::CeString);
                        }
                        GeneratedField::CeBytes => {
                            if attr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ceBytes"));
                            }
                            attr__ = map_.next_value::<::std::option::Option<::pbjson::private::BytesDeserialize<_>>>()?.map(|x| cloud_event::cloud_event_attribute_value::Attr::CeBytes(x.0));
                        }
                        GeneratedField::CeUri => {
                            if attr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ceUri"));
                            }
                            attr__ = map_.next_value::<::std::option::Option<_>>()?.map(cloud_event::cloud_event_attribute_value::Attr::CeUri);
                        }
                        GeneratedField::CeUriRef => {
                            if attr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ceUriRef"));
                            }
                            attr__ = map_.next_value::<::std::option::Option<_>>()?.map(cloud_event::cloud_event_attribute_value::Attr::CeUriRef);
                        }
                        GeneratedField::CeTimestamp => {
                            if attr__.is_some() {
                                return Err(serde::de::Error::duplicate_field("ceTimestamp"));
                            }
                            attr__ = map_.next_value::<::std::option::Option<_>>()?.map(cloud_event::cloud_event_attribute_value::Attr::CeTimestamp)
;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(cloud_event::CloudEventAttributeValue {
                    attr: attr__,
                })
            }
        }
        deserializer.deserialize_struct("caspers.messages.v1.CloudEvent.CloudEventAttributeValue", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for CloudEventBatch {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.events.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("caspers.messages.v1.CloudEventBatch", len)?;
        if !self.events.is_empty() {
            struct_ser.serialize_field("events", &self.events)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for CloudEventBatch {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "events",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Events,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "events" => Ok(GeneratedField::Events),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = CloudEventBatch;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct caspers.messages.v1.CloudEventBatch")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<CloudEventBatch, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut events__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Events => {
                            if events__.is_some() {
                                return Err(serde::de::Error::duplicate_field("events"));
                            }
                            events__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(CloudEventBatch {
                    events: events__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("caspers.messages.v1.CloudEventBatch", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for LineItem {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if !self.product_id.is_empty() {
            len += 1;
        }
        if self.quantity != 0 {
            len += 1;
        }
        if self.price != 0. {
            len += 1;
        }
        if self.total_price != 0. {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("caspers.messages.v1.LineItem", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if !self.product_id.is_empty() {
            struct_ser.serialize_field("product_id", &self.product_id)?;
        }
        if self.quantity != 0 {
            struct_ser.serialize_field("quantity", &self.quantity)?;
        }
        if self.price != 0. {
            struct_ser.serialize_field("price", &self.price)?;
        }
        if self.total_price != 0. {
            struct_ser.serialize_field("total_price", &self.total_price)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for LineItem {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "product_id",
            "productId",
            "quantity",
            "price",
            "total_price",
            "totalPrice",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            ProductId,
            Quantity,
            Price,
            TotalPrice,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "productId" | "product_id" => Ok(GeneratedField::ProductId),
                            "quantity" => Ok(GeneratedField::Quantity),
                            "price" => Ok(GeneratedField::Price),
                            "totalPrice" | "total_price" => Ok(GeneratedField::TotalPrice),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = LineItem;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct caspers.messages.v1.LineItem")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<LineItem, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut product_id__ = None;
                let mut quantity__ = None;
                let mut price__ = None;
                let mut total_price__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::ProductId => {
                            if product_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("productId"));
                            }
                            product_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Quantity => {
                            if quantity__.is_some() {
                                return Err(serde::de::Error::duplicate_field("quantity"));
                            }
                            quantity__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::Price => {
                            if price__.is_some() {
                                return Err(serde::de::Error::duplicate_field("price"));
                            }
                            price__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::TotalPrice => {
                            if total_price__.is_some() {
                                return Err(serde::de::Error::duplicate_field("totalPrice"));
                            }
                            total_price__ = 
                                Some(map_.next_value::<::pbjson::private::NumberDeserialize<_>>()?.0)
                            ;
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(LineItem {
                    id: id__.unwrap_or_default(),
                    product_id: product_id__.unwrap_or_default(),
                    quantity: quantity__.unwrap_or_default(),
                    price: price__.unwrap_or_default(),
                    total_price: total_price__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("caspers.messages.v1.LineItem", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Order {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if !self.customer_id.is_empty() {
            len += 1;
        }
        if !self.line_items.is_empty() {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("caspers.messages.v1.Order", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if !self.customer_id.is_empty() {
            struct_ser.serialize_field("customer_id", &self.customer_id)?;
        }
        if !self.line_items.is_empty() {
            struct_ser.serialize_field("line_items", &self.line_items)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for Order {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "customer_id",
            "customerId",
            "line_items",
            "lineItems",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            CustomerId,
            LineItems,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "customerId" | "customer_id" => Ok(GeneratedField::CustomerId),
                            "lineItems" | "line_items" => Ok(GeneratedField::LineItems),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Order;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct caspers.messages.v1.Order")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<Order, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut customer_id__ = None;
                let mut line_items__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::CustomerId => {
                            if customer_id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("customerId"));
                            }
                            customer_id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::LineItems => {
                            if line_items__.is_some() {
                                return Err(serde::de::Error::duplicate_field("lineItems"));
                            }
                            line_items__ = Some(map_.next_value()?);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(Order {
                    id: id__.unwrap_or_default(),
                    customer_id: customer_id__.unwrap_or_default(),
                    line_items: line_items__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("caspers.messages.v1.Order", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for OrderStatus {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut len = 0;
        if !self.id.is_empty() {
            len += 1;
        }
        if self.status != 0 {
            len += 1;
        }
        let mut struct_ser = serializer.serialize_struct("caspers.messages.v1.OrderStatus", len)?;
        if !self.id.is_empty() {
            struct_ser.serialize_field("id", &self.id)?;
        }
        if self.status != 0 {
            let v = Status::try_from(self.status)
                .map_err(|_| serde::ser::Error::custom(format!("Invalid variant {}", self.status)))?;
            struct_ser.serialize_field("status", &v)?;
        }
        struct_ser.end()
    }
}
impl<'de> serde::Deserialize<'de> for OrderStatus {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "id",
            "status",
        ];

        #[allow(clippy::enum_variant_names)]
        enum GeneratedField {
            Id,
            Status,
            __SkipField__,
        }
        impl<'de> serde::Deserialize<'de> for GeneratedField {
            fn deserialize<D>(deserializer: D) -> std::result::Result<GeneratedField, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct GeneratedVisitor;

                impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
                    type Value = GeneratedField;

                    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(formatter, "expected one of: {:?}", &FIELDS)
                    }

                    #[allow(unused_variables)]
                    fn visit_str<E>(self, value: &str) -> std::result::Result<GeneratedField, E>
                    where
                        E: serde::de::Error,
                    {
                        match value {
                            "id" => Ok(GeneratedField::Id),
                            "status" => Ok(GeneratedField::Status),
                            _ => Ok(GeneratedField::__SkipField__),
                        }
                    }
                }
                deserializer.deserialize_identifier(GeneratedVisitor)
            }
        }
        struct GeneratedVisitor;
        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = OrderStatus;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("struct caspers.messages.v1.OrderStatus")
            }

            fn visit_map<V>(self, mut map_: V) -> std::result::Result<OrderStatus, V::Error>
                where
                    V: serde::de::MapAccess<'de>,
            {
                let mut id__ = None;
                let mut status__ = None;
                while let Some(k) = map_.next_key()? {
                    match k {
                        GeneratedField::Id => {
                            if id__.is_some() {
                                return Err(serde::de::Error::duplicate_field("id"));
                            }
                            id__ = Some(map_.next_value()?);
                        }
                        GeneratedField::Status => {
                            if status__.is_some() {
                                return Err(serde::de::Error::duplicate_field("status"));
                            }
                            status__ = Some(map_.next_value::<Status>()? as i32);
                        }
                        GeneratedField::__SkipField__ => {
                            let _ = map_.next_value::<serde::de::IgnoredAny>()?;
                        }
                    }
                }
                Ok(OrderStatus {
                    id: id__.unwrap_or_default(),
                    status: status__.unwrap_or_default(),
                })
            }
        }
        deserializer.deserialize_struct("caspers.messages.v1.OrderStatus", FIELDS, GeneratedVisitor)
    }
}
impl serde::Serialize for Status {
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let variant = match self {
            Self::Unspecified => "STATUS_UNSPECIFIED",
            Self::Received => "STATUS_RECEIVED",
            Self::Accepted => "STATUS_ACCEPTED",
            Self::Processing => "STATUS_PROCESSING",
            Self::Ready => "STATUS_READY",
            Self::PickedUp => "STATUS_PICKED_UP",
            Self::Delivered => "STATUS_DELIVERED",
            Self::Cancelled => "STATUS_CANCELLED",
        };
        serializer.serialize_str(variant)
    }
}
impl<'de> serde::Deserialize<'de> for Status {
    #[allow(deprecated)]
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        const FIELDS: &[&str] = &[
            "STATUS_UNSPECIFIED",
            "STATUS_RECEIVED",
            "STATUS_ACCEPTED",
            "STATUS_PROCESSING",
            "STATUS_READY",
            "STATUS_PICKED_UP",
            "STATUS_DELIVERED",
            "STATUS_CANCELLED",
        ];

        struct GeneratedVisitor;

        impl<'de> serde::de::Visitor<'de> for GeneratedVisitor {
            type Value = Status;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(formatter, "expected one of: {:?}", &FIELDS)
            }

            fn visit_i64<E>(self, v: i64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Signed(v), &self)
                    })
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                i32::try_from(v)
                    .ok()
                    .and_then(|x| x.try_into().ok())
                    .ok_or_else(|| {
                        serde::de::Error::invalid_value(serde::de::Unexpected::Unsigned(v), &self)
                    })
            }

            fn visit_str<E>(self, value: &str) -> std::result::Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match value {
                    "STATUS_UNSPECIFIED" => Ok(Status::Unspecified),
                    "STATUS_RECEIVED" => Ok(Status::Received),
                    "STATUS_ACCEPTED" => Ok(Status::Accepted),
                    "STATUS_PROCESSING" => Ok(Status::Processing),
                    "STATUS_READY" => Ok(Status::Ready),
                    "STATUS_PICKED_UP" => Ok(Status::PickedUp),
                    "STATUS_DELIVERED" => Ok(Status::Delivered),
                    "STATUS_CANCELLED" => Ok(Status::Cancelled),
                    _ => Err(serde::de::Error::unknown_variant(value, FIELDS)),
                }
            }
        }
        deserializer.deserialize_any(GeneratedVisitor)
    }
}
