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

//! Index specification about the index built on vertex/edge.

use crate::raw::RawIndexSpec;
use std::fmt::Display;

/// `IndexSpec` describes the characteristics of a particular index.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct IndexSpec {
    /// The label of the index built on
    pub label: String,
    /// The field of the index built on
    pub field: String,
    /// Whether the index is a unique index
    pub unique: bool,
}

impl IndexSpec {
    pub(crate) fn from_raw_index_spec(raw: &RawIndexSpec) -> IndexSpec {
        IndexSpec {
            label: raw.label(),
            field: raw.field(),
            unique: raw.unique(),
        }
    }
}

impl Display for IndexSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
