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

use crate::ffi;

raw_pod_rustlize!(
    RawEdgeUid,
    lgraph_api_edge_uid_t,
    lgraph_api_edge_euid_destroy,
);

impl RawEdgeUid {
    pub(crate) fn src(&self) -> i64 {
        unsafe { ffi::lgraph_api_edge_euid_get_src(self.inner) }
    }

    pub(crate) fn lid(&self) -> u16 {
        unsafe { ffi::lgraph_api_edge_euid_get_lid(self.inner) }
    }

    pub(crate) fn tid(&self) -> i64 {
        unsafe { ffi::lgraph_api_edge_euid_get_tid(self.inner) }
    }

    pub(crate) fn eid(&self) -> i64 {
        unsafe { ffi::lgraph_api_edge_euid_get_eid(self.inner) }
    }

    pub(crate) fn dst(&self) -> i64 {
        unsafe { ffi::lgraph_api_edge_euid_get_dst(self.inner) }
    }
}

raw_pod_rustlize!(RawDate, lgraph_api_date_t, lgraph_api_date_destroy);
impl RawDate {
    pub(crate) fn new() -> Self {
        unsafe {
            RawDate {
                inner: ffi::lgraph_api_create_date(),
            }
        }
    }

    pub(crate) fn from_days(days: i32) -> Self {
        unsafe {
            RawDate {
                inner: ffi::lgraph_api_create_date_days(days),
            }
        }
    }

    pub(crate) fn from_ymd(year: i32, month: u32, day: u32) -> Self {
        unsafe {
            RawDate {
                inner: ffi::lgraph_api_create_date_ymd(year, month, day),
            }
        }
    }

    pub(crate) fn days_since_epoch(&self) -> i32 {
        unsafe { ffi::lgraph_api_date_days_since_epoch(self.inner) }
    }
}

impl Default for RawDate {
    fn default() -> Self {
        Self::new()
    }
}

raw_pod_rustlize!(
    RawDateTime,
    lgraph_api_date_time_t,
    lgraph_api_date_time_destroy,
);

impl RawDateTime {
    pub(crate) fn new() -> Self {
        unsafe {
            RawDateTime {
                inner: ffi::lgraph_api_create_date_time(),
            }
        }
    }

    pub(crate) fn from_ymdhms(
        year: i32,
        month: u32,
        day: u32,
        hour: u32,
        minute: u32,
        second: u32,
    ) -> Self {
        unsafe {
            RawDateTime {
                inner: ffi::lgraph_api_create_date_time_ymdhms(
                    year, month, day, hour, minute, second,
                ),
            }
        }
    }

    pub(crate) fn from_seconds_since_epoch(seconds: i64) -> Self {
        unsafe {
            RawDateTime {
                inner: ffi::lgraph_api_create_date_time_seconds(seconds),
            }
        }
    }

    pub(crate) fn seconds_since_epoch(&self) -> i64 {
        unsafe { ffi::lgraph_api_date_time_seconds_since_epoch(self.inner) }
    }
}

impl Default for RawDateTime {
    fn default() -> Self {
        Self::new()
    }
}
