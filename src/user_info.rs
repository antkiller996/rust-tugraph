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

use std::{collections::HashSet, fmt::Display};

use crate::raw::RawUserInfo;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UserInfo {
    pub desc: String,
    pub roles: HashSet<String>,
    pub disabled: bool,
    pub memory_limit: usize,
}

impl Display for UserInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl UserInfo {
    pub(crate) fn from_raw(raw: &RawUserInfo) -> Self {
        UserInfo {
            desc: raw.desc(),
            roles: raw.roles(),
            disabled: raw.disabled(),
            memory_limit: raw.memory_limit(),
        }
    }
}
