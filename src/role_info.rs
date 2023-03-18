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

use std::collections::HashMap;

use crate::{raw::RawRoleInfo, types::AccessLevel};

/// `RoleInfo` describe the role information of database
#[derive(Debug, Clone)]
pub struct RoleInfo {
    /// The Description of the role.
    pub desc: String,
    /// The access level of each graph. e.g. "default": [`AccessLevel::Read`]
    /// represents the role has read access level of "default" graph.
    pub graph_access: HashMap<String, AccessLevel>,
    /// Whether the role is disabled in database
    pub disabled: bool,
}

impl RoleInfo {
    pub(crate) fn from_raw(raw: &RawRoleInfo) -> Self {
        RoleInfo {
            desc: raw.desc(),
            graph_access: raw.graph_access(),
            disabled: raw.disabled(),
        }
    }
}
