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

use tugraph_plugin_util::tugraph_plugin;
use tugraph::{db::Graph, txn::TxnRead, Result};

#[tugraph_plugin]
fn echo_with_num_vertex(graph: &mut Graph, request: &str) -> Result<String> {
    let ro_txn = graph.create_ro_txn()?;
    let num_vertex = ro_txn.num_vertices()?;
    Ok(format!(
        "Request: {request}, Response: vertex num {num_vertex}"
    ))
}
