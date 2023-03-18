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

use std::{
    fmt::{Debug, Display},
    ops::Deref,
};

use chrono::Datelike;

use crate::{
    ffi,
    raw::{RawDate, RawDateTime, RawEdgeUid},
};

/// `EdgeUid` is the primary key, or key for short, of Edge element.
#[derive(Clone, Copy, Default, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EdgeUid {
    /// The key of source vertex.
    pub src: i64,
    /// The label id of the label of the Edge.
    pub lid: u16,
    /// The temporal id, which can be used as rank key. For example, transfer time.
    pub tid: i64,
    /// The key of destination vertex.
    pub dst: i64,
    /// The id of edge which aims to distinguish when src, lid, tid, dst are all same.
    pub eid: i64,
}

impl EdgeUid {
    pub(crate) fn from_raw(raw: &RawEdgeUid) -> Self {
        unsafe {
            let (src, lid, tid, dst, eid) = (
                ffi::lgraph_api_edge_euid_get_src(raw.as_ptr_mut()),
                ffi::lgraph_api_edge_euid_get_lid(raw.as_ptr_mut()),
                ffi::lgraph_api_edge_euid_get_tid(raw.as_ptr_mut()),
                ffi::lgraph_api_edge_euid_get_dst(raw.as_ptr_mut()),
                ffi::lgraph_api_edge_euid_get_eid(raw.as_ptr_mut()),
            );

            EdgeUid {
                src,
                lid,
                tid,
                dst,
                eid,
            }
        }
    }

    pub(crate) fn as_raw(&self) -> RawEdgeUid {
        unsafe {
            let ptr =
                ffi::lgraph_api_create_edge_euid(self.src, self.dst, self.lid, self.tid, self.eid);
            RawEdgeUid::from_ptr(ptr)
        }
    }
}

impl Display for EdgeUid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// `AccessLevel` is a type that represents all possible priorities of database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum AccessLevel {
    /// Neither can read nor write
    #[default]
    None = ffi::lgraph_api_access_level_none as isize,
    /// Read only
    Read = ffi::lgraph_api_access_level_read as isize,
    /// Read write
    Write = ffi::lgraph_api_access_level_write as isize,
    /// Read write and all administrate operations
    Full = ffi::lgraph_api_access_level_full as isize,
}

impl TryFrom<u32> for AccessLevel {
    type Error = crate::Error;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            ffi::lgraph_api_access_level_none => Ok(Self::None),
            ffi::lgraph_api_access_level_read => Ok(Self::Read),
            ffi::lgraph_api_access_level_write => Ok(Self::Write),
            ffi::lgraph_api_access_level_full => Ok(Self::Full),
            _ => Err(crate::Error::new("Invalid parameter.".to_string())),
        }
    }
}

impl Display for AccessLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// ISO 8601 calendar date without timezone.
///
/// See the [`crate::field::FieldData`] for details
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Date {
    inner: chrono::NaiveDate,
}

impl Deref for Date {
    type Target = chrono::NaiveDate;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AsRef<chrono::NaiveDate> for Date {
    fn as_ref(&self) -> &chrono::NaiveDate {
        &self.inner
    }
}

impl Date {
    pub fn from_native(native: chrono::NaiveDate) -> Self {
        Date { inner: native }
    }

    pub(crate) fn from_raw_date(raw: &RawDate) -> Self {
        Date {
            inner: chrono::NaiveDate::from_num_days_from_ce_opt(raw.days_since_epoch()).unwrap(),
        }
    }

    pub(crate) fn as_raw_date(&self) -> RawDate {
        RawDate::from_days(self.num_days_from_ce())
    }
}

impl Debug for Date {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl Display for Date {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

/// ISO 8601 combined date and time without timezone.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Hash, Default)]
#[repr(transparent)]
pub struct DateTime {
    inner: chrono::NaiveDateTime,
}

impl Deref for DateTime {
    type Target = chrono::NaiveDateTime;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AsRef<chrono::NaiveDateTime> for DateTime {
    fn as_ref(&self) -> &chrono::NaiveDateTime {
        &self.inner
    }
}

impl DateTime {
    /// Create from a [`chrono::NaiveDateTime`]. Actually DateTime is implemented in term of that.
    ///
    /// # Examples
    ///
    /// ```
    /// use chrono::{NaiveDate, Datelike, Timelike, Weekday};
    /// use tugraph::types::DateTime;
    ///
    /// let native = NaiveDate::from_ymd_opt(2016, 7, 8).unwrap().and_hms_opt(9, 10, 11).unwrap();
    /// let dt = DateTime::from_native(native);
    /// // Since DataTime implement Deref<Target = chrono::NaiveDateTime> trait,
    /// // you can use methods from NaiveDateTime
    /// assert_eq!(dt.weekday(), Weekday::Fri);
    /// assert_eq!(dt.num_seconds_from_midnight(), 33011);
    pub fn from_native(native: chrono::NaiveDateTime) -> Self {
        DateTime { inner: native }
    }

    /// Create from the number of non-leap seconds
    /// since the midnight UTC on January 1, 1970 (aka "UNIX timestamp")
    ///
    /// # Exmaples
    ///
    /// ```
    /// use tugraph::types::DateTime;
    /// use std::i64;
    ///
    /// let from_timestamp_opt = DateTime::from_timestamp_opt;
    ///
    /// assert!(from_timestamp_opt(0).is_some());
    /// assert!(from_timestamp_opt(1000_000_000).is_some());
    /// assert!(from_timestamp_opt(i64::MAX).is_none());
    /// ```
    pub fn from_timestamp_opt(secs: i64) -> Option<Self> {
        chrono::NaiveDateTime::from_timestamp_opt(secs, 0).map(Self::from_native)
    }

    pub(crate) fn from_raw_datetime(raw: &RawDateTime) -> Self {
        DateTime {
            inner: chrono::NaiveDateTime::from_timestamp_opt(raw.seconds_since_epoch(), 0).unwrap(),
        }
    }

    pub(crate) fn as_raw_datetime(&self) -> RawDateTime {
        RawDateTime::from_seconds_since_epoch(self.timestamp())
    }
}

impl Debug for DateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.inner)
    }
}

impl Display for DateTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[cfg(test)]
mod tests {
    use super::{AccessLevel, Date, DateTime, EdgeUid};
    use chrono::Datelike;

    #[test]
    fn test_edge_uid() {
        let euid = EdgeUid {
            src: 1,
            lid: 1,
            tid: 1,
            dst: 1,
            eid: 1,
        };
        let raw_euid = euid.as_raw();
        assert_eq!(raw_euid.src(), 1);
        assert_eq!(raw_euid.lid(), 1);
        assert_eq!(raw_euid.tid(), 1);
        assert_eq!(raw_euid.dst(), 1);
        assert_eq!(raw_euid.eid(), 1);
    }

    #[test]
    fn test_access_level() {
        let none: AccessLevel = libtugraph_sys::lgraph_api_access_level_none
            .try_into()
            .expect("faield to convert access_level_none");
        assert!(matches!(none, AccessLevel::None));
        let read: AccessLevel = libtugraph_sys::lgraph_api_access_level_read
            .try_into()
            .expect("faield to convert access_level_read");
        assert!(matches!(read, AccessLevel::Read));
        let write: AccessLevel = libtugraph_sys::lgraph_api_access_level_write
            .try_into()
            .expect("faield to convert access_level_write");
        assert!(matches!(write, AccessLevel::Write));
        let full: AccessLevel = libtugraph_sys::lgraph_api_access_level_full
            .try_into()
            .expect("faield to convert access_level_full");
        assert!(matches!(full, AccessLevel::Full));
        let invalid: Result<AccessLevel, _> = 4_u32.try_into();
        assert!(invalid.is_err());
    }

    #[test]
    fn test_date() {
        let date = Date::from_native(chrono::NaiveDate::from_num_days_from_ce_opt(10000).unwrap());
        let raw_date = date.as_raw_date();
        assert_eq!(raw_date.days_since_epoch(), date.num_days_from_ce());
    }

    #[test]
    fn test_datetime() {
        let datetime = DateTime::from_native(
            chrono::NaiveDateTime::from_timestamp_opt(1_000_000_000, 0).unwrap(),
        );
        let raw_datetime = datetime.as_raw_datetime();
        assert_eq!(raw_datetime.seconds_since_epoch(), datetime.timestamp());
    }
}
