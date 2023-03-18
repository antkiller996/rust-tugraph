// Copyright 2023 antkiller
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! All types describe the attributes of graph element field.

use std::fmt::Display;

use crate::{
    ffi,
    raw::{RawFieldData, RawFieldSpec},
    types::{Date, DateTime},
};

/// `FieldType` is a type that represents all possible types of field of graph element
///
/// See the [`FieldData`] for details.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FieldType {
    Null = ffi::lgraph_api_field_type_null as isize,
    Bool = ffi::lgraph_api_field_type_bool as isize,
    Int8 = ffi::lgraph_api_field_type_int8 as isize,
    Int16 = ffi::lgraph_api_field_type_int16 as isize,
    Int32 = ffi::lgraph_api_field_type_int32 as isize,
    Int64 = ffi::lgraph_api_field_type_int64 as isize,
    Float = ffi::lgraph_api_field_type_float as isize,
    Double = ffi::lgraph_api_field_type_double as isize,
    Date = ffi::lgraph_api_field_type_date as isize,
    DateTime = ffi::lgraph_api_field_type_datetime as isize,
    String = ffi::lgraph_api_field_type_string as isize,
    Blob = ffi::lgraph_api_field_type_blob as isize,
}

impl TryFrom<u32> for FieldType {
    type Error = crate::Error;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            ffi::lgraph_api_field_type_null => Ok(FieldType::Null),
            ffi::lgraph_api_field_type_bool => Ok(FieldType::Bool),
            ffi::lgraph_api_field_type_int8 => Ok(FieldType::Int8),
            ffi::lgraph_api_field_type_int16 => Ok(FieldType::Int16),
            ffi::lgraph_api_field_type_int32 => Ok(FieldType::Int32),
            ffi::lgraph_api_field_type_int64 => Ok(FieldType::Int64),
            ffi::lgraph_api_field_type_float => Ok(FieldType::Float),
            ffi::lgraph_api_field_type_double => Ok(FieldType::Double),
            ffi::lgraph_api_field_type_date => Ok(FieldType::Date),
            ffi::lgraph_api_field_type_datetime => Ok(FieldType::DateTime),
            ffi::lgraph_api_field_type_string => Ok(FieldType::String),
            ffi::lgraph_api_field_type_blob => Ok(FieldType::Blob),
            _ => Err(crate::Error::new("Invalid parameter.".to_string())),
        }
    }
}

impl From<FieldType> for u32 {
    fn from(value: FieldType) -> Self {
        value as u32
    }
}

impl Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// `FieldData` is a type that represents all possible values(and associated types) of field of graph element
#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub enum FieldData {
    #[default]
    Null,
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    Date(Date),
    DateTime(DateTime),
    String(String),
    Blob(Vec<u8>),
}

impl FieldData {
    pub(crate) fn from_raw_field_data(raw: &RawFieldData) -> Self {
        unsafe {
            match raw.ty() {
                FieldType::Null => FieldData::Null,
                FieldType::Bool => FieldData::Bool(raw.as_bool_unchecked()),
                FieldType::Int8 => FieldData::Int8(raw.as_int8_unchecked()),
                FieldType::Int16 => FieldData::Int16(raw.as_int16_unchecked()),
                FieldType::Int32 => FieldData::Int32(raw.as_int32_unchecked()),
                FieldType::Int64 => FieldData::Int64(raw.as_int64_unchecked()),
                FieldType::Float => FieldData::Float(raw.as_float_unchecked()),
                FieldType::Double => FieldData::Double(raw.as_double_unchecked()),
                FieldType::Date => FieldData::Date(Date::from_raw_date(&raw.as_date_unchecked())),
                FieldType::DateTime => {
                    FieldData::DateTime(DateTime::from_raw_datetime(&raw.as_datetime_unchecked()))
                }
                FieldType::String => FieldData::String(raw.as_string_unchecked()),
                FieldType::Blob => FieldData::Blob(raw.as_blob_unchecked()),
            }
        }
    }

    pub(crate) fn as_raw_field_data(&self) -> RawFieldData {
        match self {
            FieldData::Null => RawFieldData::new(),
            FieldData::Bool(b) => RawFieldData::from_bool(*b),
            FieldData::Int8(i) => RawFieldData::from_int8(*i),
            FieldData::Int16(i) => RawFieldData::from_int16(*i),
            FieldData::Int32(i) => RawFieldData::from_int32(*i),
            FieldData::Int64(i) => RawFieldData::from_int64(*i),
            FieldData::Float(f) => RawFieldData::from_float(*f),
            FieldData::Double(d) => RawFieldData::from_double(*d),
            FieldData::Date(date) => RawFieldData::from_date(date.as_raw_date()),
            FieldData::DateTime(datetime) => {
                RawFieldData::from_datetime(datetime.as_raw_datetime())
            }
            FieldData::String(str) => RawFieldData::from_str(str.as_str()),
            FieldData::Blob(b) => RawFieldData::from_blob(b.as_slice()),
        }
    }
}

/// `FieldSpec` describes the characteristics of a particular field.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct FieldSpec {
    /// The name of field. e.g. the age of a Person element
    pub name: String,
    /// The type of field. See the [`FieldType`] or [`FieldData`] for detials.
    pub ty: FieldType,
    /// Whether this field is optional for given element
    pub optional: bool,
}

impl Display for FieldSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FieldSpec {
    pub(crate) fn from_raw_field_spec(raw: &RawFieldSpec) -> Self {
        FieldSpec {
            name: raw.name(),
            ty: raw.ty(),
            optional: raw.optional(),
        }
    }

    pub(crate) fn as_raw_field_spec(&self) -> RawFieldSpec {
        RawFieldSpec::from_name_ty_optional(self.name.as_str(), self.ty.into(), self.optional)
    }
}

#[cfg(test)]
mod tests {
    use chrono::{NaiveDate, NaiveDateTime};

    use crate::{
        ffi,
        types::{Date, DateTime},
    };

    use super::{FieldData, FieldSpec, FieldType};
    #[test]
    fn test_field_type() {
        let t_null: FieldType = ffi::lgraph_api_field_type_null.try_into().unwrap();
        assert_eq!(t_null, FieldType::Null);
        let t_bool: FieldType = ffi::lgraph_api_field_type_bool.try_into().unwrap();
        assert_eq!(t_bool, FieldType::Bool);
        let t_int8: FieldType = ffi::lgraph_api_field_type_int8.try_into().unwrap();
        assert_eq!(t_int8, FieldType::Int8);
        let t_int16: FieldType = ffi::lgraph_api_field_type_int16.try_into().unwrap();
        assert_eq!(t_int16, FieldType::Int16);
        let t_int32: FieldType = ffi::lgraph_api_field_type_int32.try_into().unwrap();
        assert_eq!(t_int32, FieldType::Int32);
        let t_int64: FieldType = ffi::lgraph_api_field_type_int64.try_into().unwrap();
        assert_eq!(t_int64, FieldType::Int64);
        let t_float: FieldType = ffi::lgraph_api_field_type_float.try_into().unwrap();
        assert_eq!(t_float, FieldType::Float);
        let t_double: FieldType = ffi::lgraph_api_field_type_double.try_into().unwrap();
        assert_eq!(t_double, FieldType::Double);
        let t_date: FieldType = ffi::lgraph_api_field_type_date.try_into().unwrap();
        assert_eq!(t_date, FieldType::Date);
        let t_datetime: FieldType = ffi::lgraph_api_field_type_datetime.try_into().unwrap();
        assert_eq!(t_datetime, FieldType::DateTime);
        let t_string: FieldType = ffi::lgraph_api_field_type_string.try_into().unwrap();
        assert_eq!(t_string, FieldType::String);
        let t_blob: FieldType = ffi::lgraph_api_field_type_blob.try_into().unwrap();
        assert_eq!(t_blob, FieldType::Blob);
        let t_invalid: Result<FieldType, _> = 12_u32.try_into();
        assert!(t_invalid.is_err());
    }

    #[test]
    fn test_field_data() {
        let d_null = FieldData::Null;
        assert_eq!(d_null.as_raw_field_data().ty(), FieldType::Null);

        let d_bool = FieldData::Bool(true);
        let raw_bool = d_bool.as_raw_field_data();
        assert_eq!(raw_bool.ty(), FieldType::Bool);
        assert!(unsafe { raw_bool.as_bool_unchecked() });

        let d_int8 = FieldData::Int8(12);
        let raw_int8 = d_int8.as_raw_field_data();
        assert_eq!(raw_int8.ty(), FieldType::Int8);
        assert_eq!(unsafe { raw_int8.as_int8_unchecked() }, 12);

        let d_int16 = FieldData::Int16((1 << 8) + 1);
        let raw_int16 = d_int16.as_raw_field_data();
        assert_eq!(raw_int16.ty(), FieldType::Int16);
        assert_eq!(unsafe { raw_int16.as_int16_unchecked() }, (1 << 8) + 1);

        let d_int32 = FieldData::Int32((1 << 16) + 1);
        let raw_int32 = d_int32.as_raw_field_data();
        assert_eq!(raw_int32.ty(), FieldType::Int32);
        assert_eq!(unsafe { raw_int32.as_int32_unchecked() }, (1 << 16) + 1);

        let d_int64 = FieldData::Int64((1 << 32) + 1);
        let raw_int64 = d_int64.as_raw_field_data();
        assert_eq!(raw_int64.ty(), FieldType::Int64);
        assert_eq!(unsafe { raw_int64.as_int64_unchecked() }, (1 << 32) + 1);

        let d_float = FieldData::Float(1.555_f32);
        let raw_float = d_float.as_raw_field_data();
        assert_eq!(raw_float.ty(), FieldType::Float);
        assert_eq!(unsafe { raw_float.as_float_unchecked() }, 1.555f32);

        let d_double = FieldData::Double(1.555_f64);
        let raw_double = d_double.as_raw_field_data();
        assert_eq!(raw_double.ty(), FieldType::Double);
        assert_eq!(unsafe { raw_double.as_double_unchecked() }, 1.555_f64);

        let d_date = FieldData::Date(Date::from_native(
            NaiveDate::from_num_days_from_ce_opt(10000).unwrap(),
        ));
        let raw_date = d_date.as_raw_field_data();
        assert_eq!(raw_date.ty(), FieldType::Date);
        assert_eq!(
            unsafe { raw_date.as_date_unchecked() }.days_since_epoch(),
            10000
        );

        let d_datetime = FieldData::DateTime(DateTime::from_native(
            NaiveDateTime::from_timestamp_opt(1_000_000_000, 0).unwrap(),
        ));
        let raw_datetime = d_datetime.as_raw_field_data();
        assert_eq!(raw_datetime.ty(), FieldType::DateTime);
        assert_eq!(
            unsafe { raw_datetime.as_datetime_unchecked() }.seconds_since_epoch(),
            1_000_000_000
        );

        let d_string = FieldData::String("fielddata".to_string());
        let raw_string = d_string.as_raw_field_data();
        assert_eq!(raw_string.ty(), FieldType::String);
        assert_eq!(
            unsafe { raw_string.as_string_unchecked() },
            "fielddata".to_string()
        );

        let d_blob = FieldData::Blob(b"rawdata".to_vec());
        let raw_blob = d_blob.as_raw_field_data();
        assert_eq!(raw_blob.ty(), FieldType::Blob);
        assert_eq!(unsafe { raw_blob.as_blob_unchecked() }, b"rawdata".to_vec())
    }

    #[test]
    fn test_field_spec() {
        let fs = FieldSpec {
            name: "name".to_string(),
            ty: FieldType::String,
            optional: true,
        };
        let raw_fs = fs.as_raw_field_spec();
        assert_eq!(raw_fs.name(), "name".to_string());
        assert_eq!(raw_fs.ty(), FieldType::String);
        assert!(raw_fs.optional())
    }
}
