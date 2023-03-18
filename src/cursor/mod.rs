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

//! The core data read/write types which allow you move back and forth in graph.

mod edge;
mod iter;
mod vertex;

#[doc(inline)]
pub use edge::{EdgeCursor, EdgeCursorMut};
#[doc(inline)]
pub use edge::{InEdgeCur, InEdgeCurMut, OutEdgeCur, OutEdgeCurMut};
#[doc(inline)]
pub use iter::*;
#[doc(inline)]
pub use vertex::{VertexCur, VertexCurMut};
#[doc(inline)]
pub use vertex::{VertexCursor, VertexCursorMut};
