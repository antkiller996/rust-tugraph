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

//! Manages each `Graph` instance with access controlled manager `Galaxy`.

use std::path::Path;
use std::{cmp, marker::PhantomData};

use libtugraph_sys::lgraph_api_graph_db_t;

use crate::{
    field::{FieldData, FieldSpec},
    raw::{RawGalaxy, RawGraphDB},
    role_info::RoleInfo,
    txn::{RoTxn, RwTxn},
    types::{AccessLevel, EdgeUid},
    user_info::UserInfo,
    Result,
};

/// A standalone graph opened by [`Galaxy`].
///
/// See the [`Galaxy::open_graph`] for more details.
///
/// # Lifetime
/// The `'gl` lifetime of Graph is the lifetime of galaxy that is borrowed when open it.
pub struct Graph<'gl> {
    inner: RawGraphDB,
    _marker: PhantomData<&'gl Galaxy>,
}

/// The minimum max size in bytes of mmap region in which the graph mapped
pub const MINIMUM_GRAPH_MAX_SIZE: usize = 1 << 20; // 1M

impl<'gl> Graph<'gl> {
    /// Create a graph from c binding ptr
    ///
    /// # Safety
    /// It should only be used when writing rust procedure plugin to
    /// convert raw c binding ptr to rust Graph
    pub unsafe fn from_ptr(ptr: *mut lgraph_api_graph_db_t) -> Graph<'gl> {
        Graph {
            inner: RawGraphDB::from_ptr(ptr),
            _marker: PhantomData,
        }
    }

    /// Create a read-only transaction.
    ///
    /// There can be multiple simultaneously active read-only transactions but only one that
    /// can write. Once a single read-write transaction is opened, all further attempts to begin one
    /// will block until the first one is committed or aborted.  This has no effect on read-only transactions,
    /// however, and they may continue to be opened at any time.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com):
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/create_ro_txn", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", true)?;
    /// let ro_txn = graph.create_ro_txn()?;
    /// # Ok::<(), Error>(())
    /// ```
    ///
    pub fn create_ro_txn(&self) -> Result<RoTxn<'_>> {
        self.inner.create_read_txn().map(RoTxn::from_raw)
    }

    /// Create a read-write transaction.
    ///
    /// A read-write transaction can be optimistic. Optimistic transactions can run in parallel and any conflict
    /// will be detected during commit. If read-write transaction is not optimistic, only one read-write transaction can be
    /// active, all further attempts to begin one will block utils the frist one is committed or aborted.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/create_rw_txn", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// let rw_txn = graph.create_rw_txn(true)?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn create_rw_txn(&self, optimistic: bool) -> Result<RwTxn<'_>> {
        self.inner.create_write_txn(optimistic).map(RwTxn::from_raw)
    }

    /// Fork a read-only transaction
    ///
    /// The resulting read-only transaction will share the same view
    /// as the forked one, meaning that when reads are performed on the same vertex/edge,
    /// the results will always be identical, whether they are performed in the original
    /// transaction or the forked one.
    ///
    /// **Note**: Since one thread can only have one active read-only transaction, fork_ro_txn
    /// should be called in another thread.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf, thread};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/fork_ro_txn", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", true)?;
    /// let ro_txn = graph.create_ro_txn()?;
    /// thread::scope(|s| {
    ///     s.spawn(|| {
    ///         let forked_ro_txn = graph
    ///             .fork_ro_txn(&ro_txn)
    ///             .expect("fork ro-txn should be ok");
    ///     });
    /// });
    ///
    /// # Ok::<(), Error>(())
    /// ```
    #[deprecated = "transaction is not Send and cannot be used in Arc"]
    pub fn fork_ro_txn(&self, txn: &RoTxn) -> Result<RoTxn<'_>> {
        // SAFETY: The inner of RoTxn is a valid read-only transaction created by
        // `RawGraphDB::create_ro_txn`
        unsafe { self.inner.fork_txn(txn.as_raw()).map(RoTxn::from_raw) }
    }

    /// Flush buffered data to disk.
    ///
    /// Read-write transaction write data into filesystem when commit returns sucessfully.
    /// Call flush to call filesystem to write data buffered in kernel into disk.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/flush", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// let rw_txn = graph.create_rw_txn(false)?;
    /// // do some update work in rw_txn
    /// // ...
    /// graph.flush()?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn flush(&self) -> Result<()> {
        self.inner.flush()
    }

    ///  Drop all the data in the graph, including labels, indexes and vertexes/edges.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/drop_all_data", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// // do some update work in rw_txn
    /// // ...
    /// graph.drop_all_data()?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn drop_all_data(&self) -> Result<()> {
        self.inner.drop_all_data()
    }

    /// Drop all vertex and edges but keep the labels and indexes.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/drop_all_vertex", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// let rw_txn = graph.create_rw_txn(false)?;
    /// // do some update work in rw_txn
    /// // ...
    /// graph.drop_all_vertex()?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn drop_all_vertex(&self) -> Result<()> {
        self.inner.drop_all_vertex()
    }

    /// Estimate number of vertices.
    ///
    /// We don't maintain the exact number of vertices, but only the next vid.
    /// This method actually returns the next vid to be used. So if you have deleted
    /// a lot of vertices, the result can be quite different from actual number of vertices.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/estimate_num_vertices", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", true)?;
    /// let num = graph.estimate_num_vertices()?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn estimate_num_vertices(&self) -> Result<usize> {
        self.inner.estimate_num_vertices()
    }

    /// Add a vertex label.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldSpec, FieldType}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/add_vertex_label", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label(
    ///     "Person",
    ///      &[
    ///          FieldSpec {
    ///              name: "name".into(),
    ///               ty: FieldType::String,
    ///               optional: false,
    ///           },
    ///           FieldSpec {
    ///               name: "age".into(),
    ///               ty: FieldType::Int8,
    ///               optional: false,
    ///           },
    ///          FieldSpec {
    ///              name: "is_male".into(),
    ///             ty: FieldType::Bool,
    ///              optional: false,
    ///          },
    ///     ],
    ///     "name",)?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn add_vertex_label<'a, T>(
        &self,
        label: &str,
        field_specs: T,
        primary_field: &str,
    ) -> Result<bool>
    where
        T: IntoIterator<Item = &'a FieldSpec>,
    {
        let field_specs: Vec<_> = field_specs
            .into_iter()
            .map(|fs| fs.as_raw_field_spec())
            .collect();
        self.inner
            .add_vertex_label(label, &field_specs, primary_field)
    }

    /// Delete a vertex label and all the vertices with this label.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_vertex_label", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.delete_vertex_label("Person")?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn delete_vertex_label(&self, label: &str) -> Result<(bool, usize)> {
        self.inner.delete_vertex_label(label)
    }

    /// Delete fields in a vertex label.
    ///
    /// This method also updates the vertex data and indices accordingly to
    /// make sure the database remains in consistent state.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/alter_vertex_label_del_fields", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.alter_vertex_label_del_fields("Person", ["age", "is_male"])?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn alter_vertex_label_del_fields<'a, T>(
        &self,
        label: &str,
        del_fields: T,
    ) -> Result<(bool, usize)>
    where
        T: IntoIterator<Item = &'a str>,
    {
        self.inner.alter_vertex_label_del_fields(label, del_fields)
    }

    /// Add fields to a vertex label.
    ///
    /// The new fields in existing vertices will be filled
    /// with default values.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldSpec, FieldType, FieldData}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/alter_vertex_label_add_fields", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label(
    ///     "Person",
    ///      &[
    ///         FieldSpec {
    ///               name: "name".into(),
    ///               ty: FieldType::String,
    ///               optional: false,
    ///         },
    ///     ],
    ///     "name",
    /// )?;
    /// graph.alter_vertex_label_add_fields(
    ///     "Person",
    ///      &[
    ///         FieldSpec {
    ///             name: "age".into(),
    ///             ty: FieldType::Int8,
    ///             optional: false,
    ///         },
    ///     ],
    ///     &[FieldData::Bool(false)],
    /// )?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn alter_vertex_label_add_fields<'a, 'b, T, D>(
        &self,
        label: &str,
        add_fields: T,
        default_values: D,
    ) -> Result<(bool, usize)>
    where
        T: IntoIterator<Item = &'a FieldSpec>,
        D: IntoIterator<Item = &'b FieldData>,
    {
        let add_fields: Vec<_> = add_fields
            .into_iter()
            .map(|fs| fs.as_raw_field_spec())
            .collect();
        let default_values: Vec<_> = default_values
            .into_iter()
            .map(|v| v.as_raw_field_data())
            .collect();
        self.inner
            .alter_vertex_label_add_fields(label, &add_fields, &default_values)
    }

    /// Modify fields in a vertex label, either chage the data type or optional, or both.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldSpec, FieldType}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_graph/doc/alter_vertex_label_mod_fields", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label(
    ///     "Person",
    ///      &[
    ///         FieldSpec {
    ///               name: "name".into(),
    ///               ty: FieldType::String,
    ///               optional: false,
    ///         },
    ///         FieldSpec {
    ///               name: "age".into(),
    ///               ty: FieldType::Int64,
    ///               optional: false,
    ///         },
    ///     ],
    ///     "name",
    /// )?;
    /// graph.alter_vertex_label_mod_fields(
    ///     "Person",
    ///       &[
    ///           FieldSpec {
    ///               name: "age".into(),
    ///               ty: FieldType::Int8,
    ///               optional: false,
    ///           },
    ///     ],
    /// )?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn alter_vertex_label_mod_fields<'a, T>(
        &self,
        label: &str,
        mod_fields: T,
    ) -> Result<(bool, usize)>
    where
        T: IntoIterator<Item = &'a FieldSpec>,
    {
        let mod_fields: Vec<_> = mod_fields
            .into_iter()
            .map(|v| v.as_raw_field_spec())
            .collect();
        self.inner.alter_vertex_label_mod_fields(label, &mod_fields)
    }

    /// Add a edge label, specifying its schema.
    ///
    /// It is allowed to specify edge constrains, too. An edge can be bound to several
    /// (source_label, destination_label) pairs, which makes sure this type of edges will
    /// only be added between these types of vertices. By default, the constraint is empty,
    /// meaning that the edge is not restricted.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldSpec, FieldType}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/add_edge_label", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label("Person",
    ///    &[
    ///        FieldSpec {
    ///            name: "name".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "name",
    /// )?;
    /// graph.add_vertex_label("Comment",
    ///    &[
    ///        FieldSpec {
    ///            name: "content".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "content",
    /// )?;
    /// graph.add_edge_label(
    ///     "Post",
    ///     &[
    ///         FieldSpec {
    ///             name: "datetime".into(),
    ///             ty: FieldType::DateTime,
    ///             optional: false,
    ///         }
    ///     ],
    ///     "",
    ///     [("Person", "Comment")],
    /// )?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn add_edge_label<'a, 'b, 'c, U, C>(
        &self,
        label: &str,
        field_specs: U,
        temporal_field: &str,
        edge_constraints: C,
    ) -> Result<bool>
    where
        U: IntoIterator<Item = &'a FieldSpec>,
        C: IntoIterator<Item = (&'b str, &'c str)>,
    {
        let field_specs: Vec<_> = field_specs
            .into_iter()
            .map(|v| v.as_raw_field_spec())
            .collect();
        self.inner
            .add_edge_label(label, &field_specs, temporal_field, edge_constraints)
    }

    /// Deletes an edge label and all the edges of this type.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_edge_label", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.delete_edge_label("Post")?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn delete_edge_label(&self, label: &str) -> Result<(bool, usize)> {
        self.inner.delete_edge_label(label)
    }

    /// Modify edge constraint.
    ///
    /// Existing edges that violate the new constrain will be removed.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/alter_label_mod_edge_constraints", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.alter_label_mod_edge_constraints("Post", [("Person", "Comment")])?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn alter_label_mod_edge_constraints<'a, 'b, 'c, U>(
        &self,
        label: &str,
        constraints: U,
    ) -> Result<bool>
    where
        U: IntoIterator<Item = (&'b str, &'c str)>,
    {
        self.inner
            .alter_label_mod_edge_constraints(label, constraints)
    }

    /// Delete fields in an edge label.
    ///
    /// Existing edges that violate the new constrain will be removed.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/alter_edge_label_del_fields", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.alter_edge_label_del_fields("Post", ["datetime"])?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn alter_edge_label_del_fields<'a, T>(
        &self,
        label: &str,
        del_fields: T,
    ) -> Result<(bool, usize)>
    where
        T: IntoIterator<Item = &'a str>,
    {
        self.inner.alter_edge_label_del_fields(label, del_fields)
    }

    /// Add fields to an edge label.
    ///
    /// The new fields in existing edges will be set to default values.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldSpec, FieldData, FieldType}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/alter_edge_label_add_fields", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.alter_edge_label_add_fields(
    ///     "Post",
    ///     &[
    ///         FieldSpec {
    ///             name: "title".into(),
    ///             ty: FieldType::String,
    ///             optional: true,
    ///         },
    ///     ],
    ///     &[FieldData::String(String::new())],
    /// )?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn alter_edge_label_add_fields<'a, 'b, U, D>(
        &self,
        label: &str,
        add_fields: U,
        default_values: D,
    ) -> Result<(bool, usize)>
    where
        U: IntoIterator<Item = &'a FieldSpec>,
        D: IntoIterator<Item = &'b FieldData>,
    {
        let add_fields: Vec<_> = add_fields
            .into_iter()
            .map(|fs| fs.as_raw_field_spec())
            .collect();
        let default_values: Vec<_> = default_values
            .into_iter()
            .map(|v| v.as_raw_field_data())
            .collect();
        self.inner
            .alter_edge_label_add_fields(label, &add_fields, &default_values)
    }

    /// Modify fields in an edge label.
    ///
    /// Data type and OPTIONAL can be modified.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldSpec, FieldType}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/alter_edge_label_mod_fields", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.alter_edge_label_mod_fields(
    ///     "Post",
    ///     &[
    ///         FieldSpec {
    ///             name: "title".into(),
    ///             ty: FieldType::String,
    ///             optional: true,
    ///         },
    ///     ],
    /// )?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn alter_edge_label_mod_fields<'a, T>(
        &self,
        label: &str,
        mod_fields: T,
    ) -> Result<(bool, usize)>
    where
        T: IntoIterator<Item = &'a FieldSpec>,
    {
        let mod_fields: Vec<_> = mod_fields
            .into_iter()
            .map(|fs| fs.as_raw_field_spec())
            .collect();
        self.inner.alter_edge_label_mod_fields(label, &mod_fields)
    }

    /// Adds an index to 'label:field'.
    ///
    /// This method blocks until the index is fully created.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldType, FieldSpec}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_graph/doc/add_vertex_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label("Person",
    ///    &[
    ///        FieldSpec {
    ///            name: "name".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///        FieldSpec {
    ///            name: "age".into(),
    ///            ty: FieldType::Int8,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "name",
    /// )?;
    /// graph.add_vertex_index("Person", "age", false)?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn add_vertex_index(&self, label: &str, field: &str, is_unique: bool) -> Result<bool> {
        self.inner.add_vertex_index(label, field, is_unique)
    }

    /// Adds an index to 'label:field'.
    ///
    /// This method blocks until the index is fully created.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldType, FieldSpec}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/add_edge_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label("Person",
    ///    &[
    ///        FieldSpec {
    ///            name: "name".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "name",
    /// )?;
    /// graph.add_vertex_label("Comment",
    ///    &[
    ///        FieldSpec {
    ///            name: "content".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "content",
    /// )?;
    /// graph.add_edge_label(
    ///     "Post",
    ///     &[
    ///         FieldSpec {
    ///             name: "datetime".into(),
    ///             ty: FieldType::DateTime,
    ///             optional: false,
    ///         }
    ///     ],
    ///     "",
    ///     [("Person", "Comment")],
    /// )?;
    /// graph.add_edge_index("Post", "datetime", false)?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn add_edge_index(&self, label: &str, field: &str, is_unique: bool) -> Result<bool> {
        self.inner.add_edge_index(label, field, is_unique)
    }

    /// Check if this vertex_label:field is indexed.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldType, FieldSpec}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_graph/doc/is_vertex_indexed", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label("Person",
    ///    &[
    ///        FieldSpec {
    ///            name: "name".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///        FieldSpec {
    ///            name: "age".into(),
    ///            ty: FieldType::Int8,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "name",
    /// )?;
    /// graph.add_vertex_index("Person", "age", false)?;
    /// let indexed = graph.is_vertex_indexed("Person", "age")?;
    /// assert!(indexed);
    /// # Ok::<(), Error>(())
    /// ```
    pub fn is_vertex_indexed(&self, label: &str, field: &str) -> Result<bool> {
        self.inner.is_vertex_indexed(label, field)
    }

    /// Check if this edge_label:field is indexed.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldType, FieldSpec}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/is_edge_indexed", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label("Person",
    ///    &[
    ///        FieldSpec {
    ///            name: "name".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "name",
    /// )?;
    /// graph.add_vertex_label("Comment",
    ///    &[
    ///        FieldSpec {
    ///            name: "content".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "content",
    /// )?;
    /// graph.add_edge_label(
    ///     "Post",
    ///     &[
    ///         FieldSpec {
    ///             name: "datetime".into(),
    ///             ty: FieldType::DateTime,
    ///             optional: false,
    ///         }
    ///     ],
    ///     "",
    ///     [("Person", "Comment")],
    /// )?;
    /// graph.add_edge_index("Post", "datetime", false)?;
    /// let indexed = graph.is_edge_indexed("Post", "datetime")?;
    /// assert!(indexed);
    /// # Ok::<(), Error>(())
    /// ```
    pub fn is_edge_indexed(&self, label: &str, field: &str) -> Result<bool> {
        self.inner.is_edge_indexed(label, field)
    }

    /// Deletes the index to 'vertex_label:field'
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldType, FieldSpec}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_vertex_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label("Person",
    ///    &[
    ///        FieldSpec {
    ///            name: "name".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///        FieldSpec {
    ///            name: "age".into(),
    ///            ty: FieldType::Int8,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "name",
    /// )?;
    /// graph.add_vertex_index("Person", "age", false)?;
    /// let deleted = graph.delete_vertex_index("Person", "age")?;
    /// assert!(deleted);
    /// # Ok::<(), Error>(())
    /// ```
    pub fn delete_vertex_index(&self, label: &str, field: &str) -> Result<bool> {
        self.inner.delete_vertex_index(label, field)
    }

    /// Deletes the index to 'edge_label:field'
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::{FieldType, FieldSpec}, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_edge_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_label("Person",
    ///    &[
    ///        FieldSpec {
    ///            name: "name".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "name",
    /// )?;
    /// graph.add_vertex_label("Comment",
    ///    &[
    ///        FieldSpec {
    ///            name: "content".into(),
    ///            ty: FieldType::String,
    ///            optional: false,
    ///        },
    ///    ],
    ///    "content",
    /// )?;
    /// graph.add_edge_label(
    ///     "Post",
    ///     &[
    ///         FieldSpec {
    ///             name: "datetime".into(),
    ///             ty: FieldType::DateTime,
    ///             optional: false,
    ///         }
    ///     ],
    ///     "",
    ///     [("Person", "Comment")],
    /// )?;
    /// graph.add_edge_index("Post", "datetime", false)?;
    /// graph.delete_edge_index("Post", "datetime")?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn delete_edge_index(&self, label: &str, field: &str) -> Result<bool> {
        self.inner.delete_edge_index(label, field)
    }

    /// Get graph description
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/get_description", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", true)?;
    /// let desc = graph.get_description()?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn get_description(&self) -> Result<String> {
        self.inner.get_description()
    }

    /// Get maximum graph size in bytes
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/get_max_size", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", true)?;
    /// let max_size = graph.get_max_size()?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn get_max_size(&self) -> Result<usize> {
        self.inner.get_max_size()
    }

    /// Add fulltext index to 'vertex_label:field'
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Know issues
    /// There is no public api of tugraph to enable fulltext index enabled. Calling this api
    /// always return Error with message "Fulltext index is not enabled".
    ///
    /// # Examples
    /// ```no_run
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/add_vertex_full_text_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_vertex_full_text_index("Comment", "content")?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn add_vertex_full_text_index(&self, label: &str, field: &str) -> Result<bool> {
        self.inner.add_vertex_full_text_index(label, field)
    }

    /// Add fulltext index to 'edge_label:field'
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Know issues
    /// There is no public api of tugraph to enable fulltext index enabled. Calling this api
    /// always return Error with message "Fulltext index is not enabled".
    ///
    /// # Examples
    /// ```no_run
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/add_edge_full_text_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.add_edge_full_text_index("Post", "title")?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn add_edge_full_text_index(&self, label: &str, field: &str) -> Result<bool> {
        self.inner.add_edge_full_text_index(label, field)
    }

    /// Delete the fulltext index of 'vertex_label:field'
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```no_run
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_vertex_full_text_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.delete_vertex_full_text_index("Comment", "content")?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn delete_vertex_full_text_index(&self, label: &str, field: &str) -> Result<bool> {
        self.inner.delete_vertex_full_text_index(label, field)
    }

    /// Delete the fulltext index of 'edge_label:field'
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```no_run
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_edge_full_text_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.delete_vertex_full_text_index("Post", "title")?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn delete_edge_full_text_index(&self, label: &str, field: &str) -> Result<bool> {
        self.inner.delete_edge_full_text_index(label, field)
    }

    /// Rebuild the fulltext index of `vertex_labels` and `edge_labels`.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Know issues
    /// There is no public api of tugraph to enable fulltext index enabled. Calling this api
    /// always return Error with message "Fulltext index is not enabled".
    ///
    /// # Examples
    /// ```no_run
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/rebuild_full_text_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// graph.rebuild_full_text_index(["Comment"], ["Post"])?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn rebuild_full_text_index<'a, 'b, V, E>(
        &self,
        vertex_labels: V,
        edge_labels: E,
    ) -> Result<()>
    where
        V: IntoIterator<Item = &'a str>,
        E: IntoIterator<Item = &'b str>,
    {
        self.inner
            .rebuild_full_text_index(vertex_labels, edge_labels)
    }

    /// List fulltext indexes of vertex and edge.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/list_full_text_indexes", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", true)?;
    /// let full_indexes = graph.list_full_text_indexes()?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn list_full_text_indexes(&self) -> Result<Vec<ListIndex>> {
        self.inner.list_full_text_indexes().map(|v| {
            v.into_iter()
                .map(|(is_vertex, label_name, property_name)| ListIndex {
                    is_vertex,
                    label_name,
                    field_name: property_name,
                })
                .collect()
        })
    }

    /// Query vertex by fulltext index by using Lucene query syntax and return top n data.
    ///
    /// See the [Lucene Query Syntax] for details
    ///
    /// [Lucene Query Syntax]: http://lucenetutorial.com/lucene-query-syntax.html
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```no_run
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/query_vertex_by_full_text_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", true)?;
    /// let data = graph.query_vertex_by_full_text_index("Comment", "content:nice", 3)?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn query_vertex_by_full_text_index(
        &self,
        label: &str,
        query: &str,
        topn: i32,
    ) -> Result<Vec<QueryVertexFTIndex>> {
        self.inner
            .query_vertex_by_full_text_index(label, query, topn)
            .map(|v| {
                v.into_iter()
                    .map(|(vid, score)| QueryVertexFTIndex { vid, score })
                    .collect()
            })
    }

    /// Query edge by fulltext index by using Lucene query syntax and return top n data.
    ///
    /// See the [Lucene Query Syntax] for details
    ///
    /// [Lucene Query Syntax]: http://lucenetutorial.com/lucene-query-syntax.html
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```no_run
    /// use tugraph::{db::OpenOptions, field::FieldSpec, Error};
    /// use std::{str::FromStr, path::PathBuf};
    ///
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/query_edge_by_full_text_index", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", true)?;
    /// let data = graph.query_edge_by_full_text_index("Post", "date:[20020101 TO 20030101]", 3)?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn query_edge_by_full_text_index(
        &self,
        label: &str,
        query: &str,
        topn: i32,
    ) -> Result<Vec<QueryEdgeFTIndex>> {
        self.inner
            .query_edge_by_full_text_index(label, query, topn)
            .map(|v| {
                v.into_iter()
                    .map(|(raw, score)| QueryEdgeFTIndex {
                        euid: EdgeUid::from_raw(&raw),
                        score,
                    })
                    .collect()
            })
    }
}

/// Options and flags which can be used to configure how a [`Galaxy`] is opened.
///
/// This builder exposes the ability to configure how a [`Galaxy`] is opened and what
/// permissions on the opened galaxy, or more exactly,  the graphs it manages.
///
/// Generally speaking, when using `Options`, you'll first call [`OpenOptions::new`], then
/// chain calls to methods to set each option, then call [`OpenOptions::open`] to open a galaxy.
/// This will give a [`crate::Result`] with a [`Galaxy`] inside that you can further operate on.
///
/// # Examples
/// ```
/// use tugraph::{db::OpenOptions, Error};
/// let galaxy = OpenOptions::new()
///     .create(true)
///     .open("/tmp/rust_tugraph/doc/OpenOptions", "admin", "73@TuGraph")?;
/// # Ok::<(), Error>(())
/// ```
#[derive(Default)]
pub struct OpenOptions {
    durable: bool,
    create: bool,
}

impl OpenOptions {
    /// Creates a initial new set of options with `dir` where the database
    /// was created or to be created, the `username` and `password` used to open
    /// if exists.
    ///
    /// All other options not listed in parameters are default to false.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/open_new", "admin", "73@TuGraph")?;
    /// # Ok::<(), Error>(())
    pub fn new() -> Self {
        OpenOptions {
            durable: false,
            create: false,
        }
    }

    /// Whether the read-write transaction commited into database should be durable.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .durable(true)
    ///     .open("/tmp/rust_tugraph/doc/open_durable", "admin", "73@TuGraph")?;
    /// # Ok::<(), Error>(())
    pub fn durable(mut self, durable: bool) -> Self {
        self.durable = durable;
        self
    }

    /// Whether the read-write transaction commited into database should be durable.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .durable(true)
    ///     .open("/tmp/rust_tugraph/doc/open_create", "admin", "73@TuGraph")?;
    /// # Ok::<(), Error>(())
    pub fn create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Open a [`Galaxy`] with options specified by self.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .durable(true)
    ///     .open("/tmp/rust_tugraph/doc/open", "admin", "73@TuGraph")?;
    /// # Ok::<(), Error>(())
    pub fn open<P: AsRef<Path>>(self, dir: P, username: &str, password: &str) -> Result<Galaxy> {
        RawGalaxy::new_with_user(dir.as_ref(), username, password, self.durable, self.create)
            .map(|raw| Galaxy { inner: raw })
    }
}

unsafe impl Sync for Graph<'_> {}

/// ListGraph is the result of [`Galaxy::list_graphs`].
pub struct ListGraph {
    /// The name of graph. e.g. "default"
    pub name: String,
    /// The description of graph.
    pub desc: String,
    /// The max size in bytes of graph stored in filesystem.
    pub max_size: usize,
}

/// `ListIndex` is the result of [`Graph::list_full_text_indexes`].
pub struct ListIndex {
    /// Whether the index is built on a vertex.
    pub is_vertex: bool,
    /// The label which the index is built on.
    pub label_name: String,
    /// The field which the index is built on.
    pub field_name: String,
}

/// `QueryVertexFIIndex` is the result of [`Graph::query_vertex_by_full_text_index`] which represents
/// the indexed vertex and its weight.
pub struct QueryVertexFTIndex {
    /// The primary key of vertex.
    pub vid: i64,
    /// The weight of the vertex.
    pub score: f32,
}

/// `QueryEdgeFTIndex` is the result of [`Graph::query_edge_by_full_text_index`] which represents
/// the indexed edge and its weight.
pub struct QueryEdgeFTIndex {
    /// The primary key of edge.
    pub euid: EdgeUid,
    /// The weight of the edge.
    pub score: f32,
}

/// `GraphAccess` represents the access level of each graph.
pub struct GraphAccess {
    /// The graph name. e.g. "default".
    pub name: String,
    /// The access level.
    pub access_level: AccessLevel,
}

/// `ListUser` is the result of [`Galaxy::list_users`].
pub struct ListUser {
    /// The name of user.
    pub name: String,
    /// The info of user.
    pub info: UserInfo,
}

/// `ListRole` is the result of [`Galaxy::list_roles`].
pub struct ListRole {
    /// The name of role.
    pub name: String,
    /// The info of role.
    pub info: RoleInfo,
}

/// Options and flags which can be used to configure how a [`Graph`] is modified.
///
/// See the [`Galaxy::mod_graph`] for details.
///
/// # Liftime
/// lifetime `'gl` comes from the Galaxy which is borrowed when the [`Galaxy::mod_graph`] is called.
pub struct ModGraphOptions<'gl> {
    desc: Option<String>,
    max_size: Option<usize>,
    galaxy: &'gl Galaxy,
}

impl<'gl> ModGraphOptions<'gl> {
    /// Modify the graph description with given new one.
    pub fn mod_desc(mut self, new_desc: String) -> Self {
        self.desc = Some(new_desc);
        self
    }

    /// Modify the max size in bytes of graph stored in filesystem with given new one.
    pub fn mod_max_size(mut self, new_max_size: usize) -> Self {
        self.max_size = Some(new_max_size);
        self
    }

    /// Apply all modify options to graph.
    ///
    /// # Errors
    /// TODO: (eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/apply", "admin", "73@TuGraph")?;
    /// let modified = galaxy
    ///     .mod_graph()
    ///     .mod_desc("The new description of default graph".to_string())
    ///     .apply("default")?;
    /// # Ok::<(), Error>(())
    /// ```
    ///
    pub fn apply(self, graph: &str) -> Result<bool> {
        self.galaxy._mod_graph(
            graph,
            self.desc.is_some(),
            &self.desc.unwrap_or_default(),
            self.max_size.is_some(),
            self.max_size.unwrap_or_default(),
        )
    }
}

/// A `Galaxy` is the storage engine for one TuGraph instance. It manages a set of
/// User/Role/GraphDBs.
///
/// A galaxy can be opened in async mode, in which case *ALL* write transactions will be
/// treated as async, whether they declare async or not. This can come in handy if we are
/// performing a lot of writing, but can cause data loss for online processing.
pub struct Galaxy {
    inner: RawGalaxy,
}

// Why does all method use immutable receiver &self instead of mutable one &mut self ?
// Because Galaxy has interior mutability and also it is Send + Sync
// So &Galaxy or Arc<Galaxy> is safe to send across thread boundary.
// If mutable receiver &mut self is used, &Galaxy or Arc<Galaxy> cannot yield &mut Galaxy.
// Some one would say Mutex<Galaxy> can yield &mut Galaxy through MutexGuard<'_, Galaxy>.
// But Galaxy itself use mutex at all in its ffi cpp Galaxy class. Performance takes into
// considration
//
// N.B. Performance loss in single thread compared not immutable reference receiver one &self.
impl Galaxy {
    /// Open Galaxy at `dir` with username and password
    ///
    /// If more options are need to open, use [`OpenOptions`] instead.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/galaxy_open", "admin", "73@TuGraph")?;
    /// # Ok::<(), Error>(())
    pub fn open<P: AsRef<Path>>(dir: P, username: &str, password: &str) -> Result<Galaxy> {
        OpenOptions::new().open(dir, username, password)
    }

    /// Switch current user with password.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_current_user", "admin", "73@TuGraph")?;
    /// galaxy.set_current_user("test_user1", "test_password1");
    /// # Ok::<(), Error>(())
    pub fn set_current_user(&self, user: &str, password: &str) -> Result<()> {
        self.inner.set_current_user(user, password)
    }

    /// Switch current user without authorization.
    ///
    /// **Note**: It is you user's responsibility to make sure the current user
    /// have right permissions to switch.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_user_without_auth", "admin", "73@TuGraph")?;
    /// galaxy.set_user_without_auth("test_user1");
    /// # Ok::<(), Error>(())
    pub fn set_user_without_auth(&self, user: &str) -> Result<()> {
        // TODO: now set_user can switch from anyone to anyone. Is it a problem?
        self.inner.set_user(user)
    }

    /// Create graph with name, description and max size in bytes of graph stored
    /// in filesystem.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::{OpenOptions, MINIMUM_GRAPH_MAX_SIZE}, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/create_graph", "admin", "73@TuGraph")?;
    /// let _ = galaxy.create_graph(
    ///     "test_create_graph",
    ///     "graph created in test_create_graph",
    ///     MINIMUM_GRAPH_MAX_SIZE,
    /// )?;
    /// # Ok::<(), Error>(())
    pub fn create_graph(&self, name: &str, desc: &str, max_size: usize) -> Result<bool> {
        self.inner
            .create_graph(name, desc, cmp::max(max_size, MINIMUM_GRAPH_MAX_SIZE))
    }

    /// Create graph with name, description and max size in bytes of graph stored
    /// in filesystem.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::{OpenOptions, MINIMUM_GRAPH_MAX_SIZE}, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_graph", "admin", "73@TuGraph")?;
    /// let _ = galaxy.create_graph(
    ///     "test_create_graph",
    ///     "graph created in test_create_graph",
    ///     MINIMUM_GRAPH_MAX_SIZE,
    /// )?;
    /// # Ok::<(), Error>(())
    pub fn delete_graph(&self, graph: &str) -> Result<bool> {
        self.inner.delete_graph(graph)
    }

    fn _mod_graph(
        &self,
        graph: &str,
        mod_desc: bool,
        desc: &str,
        mod_size: bool,
        new_max_size: usize,
    ) -> Result<bool> {
        self.inner
            .mod_graph(graph, mod_desc, desc, mod_size, new_max_size)
    }

    /// Modify graph info.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/mod_graph", "admin", "73@TuGraph")?;
    /// let modified = galaxy
    ///     .mod_graph()
    ///     .mod_desc("The new description of default graph".to_string())
    ///     .apply("default")?;
    /// # Ok::<(), Error>(())
    pub fn mod_graph(&self) -> ModGraphOptions<'_> {
        ModGraphOptions {
            desc: None,
            max_size: None,
            galaxy: self,
        }
    }

    /// List graphs.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/list_graphs", "admin", "73@TuGraph")?;
    /// let graphs = galaxy.list_graphs()?;
    /// # Ok::<(), Error>(())
    pub fn list_graphs(&self) -> Result<Vec<ListGraph>> {
        self.inner.list_graphs().map(|g| {
            g.into_iter()
                .map(|(name, (desc, max_size))| ListGraph {
                    name,
                    desc,
                    max_size,
                })
                .collect()
        })
    }

    /// Create a user.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/create_user", "admin", "73@TuGraph")?;
    /// let created = galaxy.create_user("test_user1", "test_password1", "user one")?;
    /// # Ok::<(), Error>(())
    pub fn create_user(&self, username: &str, password: &str, desc: &str) -> Result<bool> {
        self.inner.create_user(username, password, desc)
    }

    /// Delete a user.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_user", "admin", "73@TuGraph")?;
    /// galaxy.create_user("test_user1", "test_password1", "user one")?;
    /// let deleted = galaxy.delete_user("test_user1")?;
    /// assert!(deleted);
    /// # Ok::<(), Error>(())
    pub fn delete_user(&self, username: &str) -> Result<bool> {
        self.inner.delete_user(username)
    }

    /// Set the password of the specified user.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_password", "admin", "73@TuGraph")?;
    /// galaxy.set_password("test_user1", "test_password1", "new_password1")?;
    /// # Ok::<(), Error>(())
    pub fn set_password(
        &self,
        username: &str,
        old_password: &str,
        new_password: &str,
    ) -> Result<bool> {
        self.inner
            .set_password(username, old_password, new_password)
    }

    /// Set user description.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_user_desc", "admin", "73@TuGraph")?;
    /// galaxy.set_user_desc("test_user1", "new_description")?;
    /// # Ok::<(), Error>(())
    pub fn set_user_desc(&self, username: &str, desc: &str) -> Result<bool> {
        self.inner.set_user_desc(username, desc)
    }

    /// Set the roles of the specified user. If you need to add or delete a role, you
    /// will need to use GetUserInfo to get the roles first.
    ///
    /// **Note**: Only admin can set user roles.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_user_roles", "admin", "73@TuGraph")?;
    /// galaxy.create_user("test_user1", "test_password1", "test user one")?;
    /// galaxy.create_role("operator", "A good operator")?;
    /// galaxy.create_role("crud_boy", "Create, retrieve, update and delete data all day")?;
    /// galaxy.set_user_roles("test_user1", ["operator", "crud_boy"])?;
    /// # Ok::<(), Error>(())
    /// ```
    pub fn set_user_roles<'a, R>(&self, username: &str, roles: R) -> Result<bool>
    where
        R: IntoIterator<Item = &'a str>,
    {
        self.inner.set_user_roles(username, roles)
    }

    /// Sets user access rights on a graph.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, types::AccessLevel, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_user_graph_access", "admin", "73@TuGraph")?;
    /// galaxy
    ///     .set_user_graph_access("test_user1", "default", AccessLevel::Full)?;
    /// # Ok::<(), Error>(())
    pub fn set_user_graph_access(
        &self,
        username: &str,
        graph: &str,
        access: AccessLevel,
    ) -> Result<bool> {
        self.inner.set_user_graph_access(username, graph, access)
    }

    /// Disable a user. A disabled user is not able to login or perform any operation. A
    /// user cannot disable itself.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/disable_user", "admin", "73@TuGraph")?;
    /// galaxy.create_user("test_user1", "test_password1", "user one")?;
    /// galaxy.disable_user("test_user1")?;
    /// # Ok::<(), Error>(())
    pub fn disable_user(&self, username: &str) -> Result<bool> {
        self.inner.disable_user(username)
    }

    /// Enable a user.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/enable_user", "admin", "73@TuGraph")?;
    /// galaxy.create_user("test_user1", "test_password1", "user one")?;
    /// galaxy.disable_user("test_user1")?;
    /// galaxy.enable_user("test_user1")?;
    /// # Ok::<(), Error>(())
    pub fn enable_user(&self, username: &str) -> Result<bool> {
        self.inner.enable_user(username)
    }

    /// List all users
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/list_users", "admin", "73@TuGraph")?;
    /// galaxy
    ///     .create_user("test_user1", "test_password1", "user one")?;
    /// galaxy
    ///     .create_user("test_user2", "test_password2", "user two")?;
    /// let users = galaxy.list_users()?;
    /// # Ok::<(), Error>(())
    pub fn list_users(&self) -> Result<Vec<ListUser>> {
        self.inner.list_users().map(|v| {
            v.into_iter()
                .map(|(name, info)| ListUser {
                    name,
                    info: UserInfo::from_raw(&info),
                })
                .collect()
        })
    }

    /// Get user information
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/get_user_info", "admin", "73@TuGraph")?;
    /// galaxy
    ///     .create_user("test_user1", "test_password1", "user one")?;
    /// let info = galaxy.get_user_info("test_user1")?;
    /// # Ok::<(), Error>(())
    pub fn get_user_info(&self, username: &str) -> Result<UserInfo> {
        self.inner
            .get_user_info(username)
            .map(|info| UserInfo::from_raw(&info))
    }

    /// Create a role. A role has different access levels to different graphs. Every user
    /// must be assigned some role to get access to graphs.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/create_role", "admin", "73@TuGraph")?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// # Ok::<(), Error>(())
    pub fn create_role(&self, role: &str, desc: &str) -> Result<bool> {
        self.inner.create_role(role, desc)
    }

    /// Deletes the role.
    ///
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/delete_role", "admin", "73@TuGraph")?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// galaxy.delete_role("operator")?;
    /// # Ok::<(), Error>(())
    pub fn delete_role(&self, role: &str) -> Result<bool> {
        self.inner.delete_role(role)
    }

    /// Disable a role.
    ///
    /// A disabled role still has the data, but is not effective.
    /// i.e., users will not have access rights to graphs that are obtained by having
    /// this role.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/disable_role", "admin", "73@TuGraph")?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// galaxy.disable_role("operator")?;
    /// # Ok::<(), Error>(())
    pub fn disable_role(&self, role: &str) -> Result<bool> {
        self.inner.disable_role(role)
    }

    /// Enable the role.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy =  OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/enable_role", "admin", "73@TuGraph")?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// galaxy.disable_role("operator")?;
    /// galaxy.enable_role("operator")?;
    /// # Ok::<(), Error>(())
    pub fn enable_role(&self, role: &str) -> Result<bool> {
        self.inner.enable_role(role)
    }

    /// Set the description of the specified role
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_role_desc", "admin", "73@TuGraph")?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// galaxy.set_role_desc("operator", "a good operator")?;
    /// # Ok::<(), Error>(())
    pub fn set_role_desc(&self, role: &str, desc: &str) -> Result<bool> {
        self.inner.set_role_desc(role, desc)
    }

    /// Set access of the role to graphs.
    ///
    /// If you need to add or remove access to part of the graphs, you need
    /// to get full graph_access map by using GetRoleInfo first.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{
    ///     db::{OpenOptions, GraphAccess},
    ///     types::AccessLevel,
    ///     Error,
    /// };
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_role_access_rights", "admin", "73@TuGraph")?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// galaxy.set_role_access_rights(
    ///     "operator",
    ///     &[GraphAccess {
    ///         name: "default".to_string(),
    ///         access_level: AccessLevel::Full,
    ///     }],
    /// )?;
    /// # Ok::<(), Error>(())
    pub fn set_role_access_rights<'a, T>(&self, role: &str, graph_access: T) -> Result<bool>
    where
        T: IntoIterator<Item = &'a GraphAccess>,
    {
        self.inner.set_role_access_rights(
            role,
            graph_access
                .into_iter()
                .map(|ga| (ga.name.as_str(), ga.access_level)),
        )
    }

    /// Incrementally modify the access right of the specified role.
    ///
    /// For example, for a role that has access right {graph1:READ, graph2:WRITE},
    /// calling this function with graph_access={graph2:READ, graph3:FULL}
    /// will set the access right of this role to {graph1:READ, graph2:READ, graph3:FULL}
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{
    ///     db::{OpenOptions, GraphAccess, MINIMUM_GRAPH_MAX_SIZE},
    ///     types::AccessLevel,
    ///     Error,
    /// };
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/set_role_access_rights_incremental", "admin", "73@TuGraph")?;
    /// galaxy
    ///     .create_graph("test_graph1", "test graph one", MINIMUM_GRAPH_MAX_SIZE)?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// galaxy.set_role_access_rights_incremental(
    ///     "operator",
    ///     &[GraphAccess {
    ///         name: "default".to_string(),
    ///         access_level: AccessLevel::Write,
    ///     }],
    /// )?;
    /// galaxy.set_role_access_rights_incremental(
    ///     "operator",
    ///     &[GraphAccess {
    ///         name: "test_graph1".to_string(),
    ///         access_level: AccessLevel::Read,
    ///     }],
    /// )?;
    /// # Ok::<(), Error>(())
    pub fn set_role_access_rights_incremental<'a, T>(
        &self,
        role: &str,
        graph_access: T,
    ) -> Result<bool>
    where
        T: IntoIterator<Item = &'a GraphAccess>,
    {
        self.inner.set_role_access_rights_incremental(
            role,
            graph_access
                .into_iter()
                .map(|ga| (ga.name.as_str(), ga.access_level)),
        )
    }

    /// Gets role information.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::{OpenOptions, GraphAccess}, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/get_role_info", "admin", "73@TuGraph")?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// let info = galaxy.get_role_info("operator")?;
    /// # Ok::<(), Error>(())
    pub fn get_role_info(&self, role: &str) -> Result<RoleInfo> {
        self.inner
            .get_role_info(role)
            .map(|info| RoleInfo::from_raw(&info))
    }

    /// List all the roles.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::{OpenOptions, GraphAccess}, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/list_roles", "admin", "73@TuGraph")?;
    /// galaxy.create_role("operator", "a operator role")?;
    /// let roles: Vec<_> = galaxy.list_roles()?;
    /// # Ok::<(), Error>(())
    pub fn list_roles(&self) -> Result<Vec<ListRole>> {
        self.inner.list_roles().map(|v| {
            v.into_iter()
                .map(|(name, info)| ListRole {
                    name,
                    info: RoleInfo::from_raw(&info),
                })
                .collect()
        })
    }

    /// Get the access level that the specified user have to the graph
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::{OpenOptions, GraphAccess}, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/get_access_level", "admin", "73@TuGraph")?;
    /// galaxy.get_access_level("admin", "default")?;
    /// # Ok::<(), Error>(())
    pub fn get_access_level(&self, username: &str, graph: &str) -> Result<AccessLevel> {
        self.inner.get_access_level(username, graph)
    }

    /// Open a graph.
    ///
    /// # Errors
    /// TODO(eadrenking@outlook.com)
    ///
    /// # Examples
    /// ```
    /// use tugraph::{db::OpenOptions, Error};
    /// let galaxy = OpenOptions::new()
    ///     .create(true)
    ///     .open("/tmp/rust_tugraph/doc/open_graph", "admin", "73@TuGraph")?;
    /// let graph = galaxy.open_graph("default", false)?;
    /// # Ok::<(), Error>(())
    pub fn open_graph(&self, graph: &str, read_only: bool) -> Result<Graph<'_>> {
        // SAFETY: the underlying cpp Galaxy::OpenGraph is thread-safe
        unsafe {
            self.inner.open_graph(graph, read_only).map(|raw| Graph {
                inner: raw,
                _marker: PhantomData,
            })
        }
    }
}

// RawGalaxy is send since Galaxy in cpp side is send
unsafe impl Send for Galaxy {}
// RawGalaxy is sync since Galaxy in cpp side is sync
unsafe impl Sync for Galaxy {}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn test_mod_graph() {
        let tmpdir = tempfile::tempdir().unwrap();
        let galaxy = OpenOptions::new()
            .create(true)
            .open(tmpdir.path(), "admin", "73@TuGraph")
            .unwrap();
        let graph = galaxy.open_graph("default", true).unwrap();
        let new_desc = "The new description of default graph";
        let modified = galaxy
            .mod_graph()
            .mod_desc(new_desc.into())
            .apply("default")
            .unwrap();
        assert!(modified);
        assert_eq!(graph.get_description(), Ok(new_desc.into()));
    }

    #[test]
    fn test_admin_switch_user() {
        let tmpdir = tempfile::tempdir().unwrap();
        let galaxy = OpenOptions::new()
            .create(true)
            .open(tmpdir.path(), "admin", "73@TuGraph")
            .unwrap();
        let created = galaxy
            .create_user("test_user1", "test_password1", "user one")
            .unwrap();
        assert!(created);

        let mut usernames: Vec<_> = galaxy
            .list_users()
            .unwrap()
            .into_iter()
            .map(|u| u.name)
            .collect();
        usernames.sort();
        assert_eq!(usernames, vec!["admin", "test_user1"]);
        galaxy
            .set_user_without_auth("test_user1")
            .expect("admin should be permitted to switch to test_user1");

        galaxy
            .set_user_without_auth("admin")
            .expect("switching back from test_user1 to admin should be ok");
    }

    #[test]
    fn test_normal_user_switch_user() {
        let tmpdir = tempfile::tempdir().unwrap();

        // admin create test_user1, test_user2
        {
            let galaxy = OpenOptions::new()
                .create(true)
                .open(tmpdir.path(), "admin", "73@TuGraph")
                .unwrap();
            let created = galaxy
                .create_user("test_user1", "test_password1", "user one")
                .unwrap();
            assert!(created);
            let created = galaxy
                .create_user("test_user2", "test_password2", "user two")
                .unwrap();
            assert!(created);

            let mut usernames: Vec<_> = galaxy
                .list_users()
                .unwrap()
                .into_iter()
                .map(|u| u.name)
                .collect();
            usernames.sort();
            assert_eq!(usernames, vec!["admin", "test_user1", "test_user2"]);
        }

        // login test_user1
        let galaxy = OpenOptions::new()
            .create(true)
            .open(tmpdir.path(), "test_user1", "test_password1")
            .unwrap();

        // switch to test_user2 with password
        galaxy
            .set_current_user("test_user2", "test_password2")
            .expect("switching to test_user2 with password should be ok");

        // switch back to test_user without authorization
        galaxy
            .set_current_user("test_user1", "wrong_password")
            .expect_err("switching back to test_user1 with wrong password should be err");
    }

    #[test]
    fn test_create_user() {
        let tmpdir = tempfile::tempdir().unwrap();
        {
            let galaxy = OpenOptions::new()
                .create(true)
                .open(tmpdir.path(), "admin", "73@TuGraph")
                .unwrap();
            let created = galaxy
                .create_user("test_user1", "test_password1", "user one")
                .unwrap();
            assert!(created);
        }

        let galaxy = Galaxy::open(tmpdir.path(), "test_user1", "test_password1").unwrap();
        galaxy
            .create_user("test_user2", "test_password2", "user two")
            .expect_err("normal user cannot create new user");
    }

    #[test]
    fn test_delete_user() {
        let tmpdir = tempfile::tempdir().unwrap();
        {
            let galaxy = OpenOptions::new()
                .create(true)
                .open(tmpdir.path(), "admin", "73@TuGraph")
                .unwrap();
            let created = galaxy
                .create_user("test_user1", "test_password1", "user one")
                .unwrap();
            assert!(created);
        }
        {
            let galaxy = Galaxy::open(tmpdir.path(), "test_user1", "test_password1").unwrap();
            galaxy
                .delete_user("test_user1")
                .expect_err("normal user cannot delete user");
        }
        {
            let galaxy = Galaxy::open(tmpdir.path(), "admin", "73@TuGraph").unwrap();
            galaxy
                .delete_user("test_user1")
                .expect("admin can delete user");
        }
    }

    #[test]
    fn test_set_password() {
        let tmpdir = tempfile::tempdir().unwrap();
        {
            let galaxy = OpenOptions::new()
                .create(true)
                .open(tmpdir.path(), "admin", "73@TuGraph")
                .unwrap();
            let created = galaxy
                .create_user("test_user1", "test_password1", "user one")
                .unwrap();
            assert!(created);
            let created = galaxy
                .create_user("test_user2", "test_password2", "user one")
                .unwrap();
            assert!(created);
        }
        // admin set other users' password
        {
            let galaxy = Galaxy::open(tmpdir.path(), "admin", "73@TuGraph").unwrap();
            galaxy
                .set_password("test_user1", "test_password1", "new_password1")
                .expect("admin can set other users' password");
        }
        // normal user set other users' password and itself password
        {
            let galaxy = Galaxy::open(tmpdir.path(), "test_user1", "new_password1").unwrap();
            galaxy
                .set_password("test_user2", "test_password2", "new_password2")
                .expect_err("normal user cannot set other user's password");
            // set itself password back
            galaxy
                .set_password("test_user1", "new_password1", "test_password1")
                .expect("normal user can set itself password");
        }
    }

    #[test]
    fn test_set_user_desc() {
        let tmpdir = tempfile::tempdir().unwrap();
        {
            let galaxy = OpenOptions::new()
                .create(true)
                .open(tmpdir.path(), "admin", "73@TuGraph")
                .unwrap();
            let created = galaxy
                .create_user("test_user1", "test_password1", "user one")
                .unwrap();
            assert!(created);
            let created = galaxy
                .create_user("test_user2", "test_password2", "user one")
                .unwrap();
            assert!(created);
        }
        // admin set other users' description
        {
            let galaxy = Galaxy::open(tmpdir.path(), "admin", "73@TuGraph").unwrap();
            let setted = galaxy
                .set_user_desc("test_user1", "new_description1")
                .unwrap();
            assert!(setted);
            let desc = galaxy.get_user_info("test_user1").unwrap().desc;
            assert_eq!(desc, "new_description1");
        }
        // normal user set other users' and itself description
        {
            let galaxy = Galaxy::open(tmpdir.path(), "test_user1", "test_password1").unwrap();
            galaxy
                .set_user_desc("test_user2", "new_description2")
                .expect_err("normal user cannot set other user's description");
            let setted = galaxy
                .set_user_desc("test_user1", "the newest description1")
                .unwrap();
            assert!(setted);
            let desc = galaxy.get_user_info("test_user1").unwrap().desc;
            assert_eq!(desc, "the newest description1");
        }
    }

    #[test]
    fn test_set_user_roles() {
        let tmpdir = tempfile::tempdir().unwrap();
        {
            let galaxy = OpenOptions::new()
                .create(true)
                .open(tmpdir.path(), "admin", "73@TuGraph")
                .unwrap();
            let created = galaxy
                .create_user("test_user1", "test_password1", "user one")
                .unwrap();
            assert!(created);
            let created = galaxy
                .create_user("test_user2", "test_password2", "user one")
                .unwrap();
            assert!(created);
            let created = galaxy.create_role("operator", "a operator role").unwrap();
            assert!(created);
        }
        // admin set other users' roles
        {
            let galaxy = Galaxy::open(tmpdir.path(), "admin", "73@TuGraph").unwrap();
            let setted = galaxy.set_user_roles("test_user1", ["operator"]).unwrap();
            assert!(setted);
            let roles = galaxy.get_user_info("test_user1").unwrap().roles;
            assert_eq!(
                roles,
                HashSet::from_iter(["test_user1".to_string(), "operator".to_string()])
            );
        }
        // normal user set other users' and itself roles
        {
            let galaxy = Galaxy::open(tmpdir.path(), "test_user2", "test_password2").unwrap();
            galaxy
                .set_user_roles("test_user1", ["operator"])
                .expect_err("normal user cannot set other user's roles");
            galaxy
                .set_user_roles("test_user2", ["operator"])
                .expect_err("normal user cannot set itself roles");
        }
    }

    #[test]
    fn test_set_user_graph_access() {
        let tmpdir = tempfile::tempdir().unwrap();
        {
            let galaxy = OpenOptions::new()
                .create(true)
                .open(tmpdir.path(), "admin", "73@TuGraph")
                .unwrap();
            let created = galaxy
                .create_user("test_user1", "test_password1", "user one")
                .unwrap();
            assert!(created);
            let created = galaxy
                .create_user("test_user2", "test_password2", "user one")
                .unwrap();
            assert!(created);
        }
        // admin set other users' graph acesss level
        {
            let galaxy = Galaxy::open(tmpdir.path(), "admin", "73@TuGraph").unwrap();
            let setted = galaxy
                .set_user_graph_access("test_user1", "default", AccessLevel::Read)
                .unwrap();
            assert!(setted);
            let al = galaxy.get_access_level("test_user1", "default").unwrap();
            assert!(matches!(al, AccessLevel::Read));
        }
        // normal user set other users' and itself roles
        {
            let galaxy = Galaxy::open(tmpdir.path(), "test_user2", "test_password2").unwrap();
            galaxy
                .set_user_graph_access("test_user1", "default", AccessLevel::Write)
                .expect_err("normal user cannot set other user's access level");
            galaxy
                .set_user_graph_access("test_user2", "default", AccessLevel::Full)
                .expect_err("normal user cannot set itself roles");
        }
    }

    #[test]
    fn test_enable_disable_user() {
        let tmpdir = tempfile::tempdir().unwrap();
        {
            let galaxy = OpenOptions::new()
                .create(true)
                .open(tmpdir.path(), "admin", "73@TuGraph")
                .unwrap();
            let created = galaxy
                .create_user("test_user1", "test_password1", "user one")
                .unwrap();
            assert!(created);
            let created = galaxy
                .create_user("test_user2", "test_password2", "user one")
                .unwrap();
            assert!(created);
        }
        // normal user disable other users' and itself roles
        {
            let galaxy = Galaxy::open(tmpdir.path(), "test_user1", "test_password1").unwrap();
            galaxy
                .disable_user("test_user1")
                .expect_err("user cannot disable itself");
            galaxy
                .disable_user("test_user2")
                .expect_err("normal user cannot disable others");
        }
        // admin disable other users
        {
            let galaxy = Galaxy::open(tmpdir.path(), "admin", "73@TuGraph").unwrap();
            let disabled = galaxy.disable_user("test_user1").unwrap();
            assert!(disabled);
        }
        // try to login
        {
            assert!(Galaxy::open(tmpdir.path(), "test_user1", "test_password1").is_err());
        }
        // re-enable test_user1
        {
            let galaxy = Galaxy::open(tmpdir.path(), "admin", "73@TuGraph").unwrap();
            galaxy
                .enable_user("test_user1")
                .expect("enable user should be ok");
        }
        // try to login
        {
            assert!(Galaxy::open(tmpdir.path(), "test_user1", "test_password1").is_ok());
        }
    }

    #[test]
    fn test_role_operations() {
        let tmpdir = tempfile::tempdir().unwrap();

        let galaxy = OpenOptions::new()
            .create(true)
            .open(tmpdir.path(), "admin", "73@TuGraph")
            .unwrap();
        galaxy
            .create_graph("test_graph1", "test graph one", MINIMUM_GRAPH_MAX_SIZE)
            .expect("create graph should be ok");
        let created = galaxy
            .create_user("test_user1", "test_password1", "user one")
            .unwrap();
        assert!(created);
        let created = galaxy.create_role("operator", "a good operator").unwrap();
        assert!(created);
        let created = galaxy.create_role("developer", "C++ programmer").unwrap();
        assert!(created);
        let roles: Vec<_> = galaxy.list_roles().unwrap();
        assert_eq!(
            roles
                .iter()
                .map(|r| r.name.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["admin", "test_user1", "operator", "developer"]),
        );
        assert!(roles.iter().all(|r| !r.info.disabled));

        galaxy
            .disable_role("operator")
            .expect("disable operator should be ok");
        let operator = galaxy.get_role_info("operator").unwrap();
        assert!(operator.disabled);

        galaxy
            .enable_role("operator")
            .expect("enable operator should be ok");
        let operator = galaxy.get_role_info("operator").unwrap();
        assert!(!operator.disabled);

        galaxy
            .set_role_desc("operator", "a new operator")
            .expect("set role description should be ok");
        let operator = galaxy.get_role_info("operator").unwrap();
        assert_eq!(operator.desc, "a new operator");

        galaxy
            .set_role_access_rights(
                "operator",
                &[GraphAccess {
                    name: "default".to_string(),
                    access_level: AccessLevel::Full,
                }],
            )
            .expect("set role access rights should be ok");
        let operator = galaxy.get_role_info("operator").unwrap();
        assert!(matches!(
            operator.graph_access.get("default").unwrap(),
            AccessLevel::Full
        ));

        galaxy
            .set_role_access_rights_incremental(
                "developer",
                &[GraphAccess {
                    name: "default".to_string(),
                    access_level: AccessLevel::Read,
                }],
            )
            .expect("set role access rights incrementally should be ok");
        let developer = galaxy.get_role_info("developer").unwrap();
        assert!(matches!(
            developer.graph_access.get("default").unwrap(),
            AccessLevel::Read
        ));
        galaxy
            .set_role_access_rights_incremental(
                "developer",
                &[GraphAccess {
                    name: "test_graph1".to_string(),
                    access_level: AccessLevel::Write,
                }],
            )
            .expect("set role access rights incrementally should be ok");
        let developer = galaxy.get_role_info("developer").unwrap();
        assert!(matches!(
            developer.graph_access.get("default").unwrap(),
            AccessLevel::Read
        ));
        assert!(matches!(
            developer.graph_access.get("test_graph1").unwrap(),
            AccessLevel::Write
        ));

        galaxy
            .delete_role("developer")
            .expect("delete a role should be ok");
        let roles: Vec<_> = galaxy.list_roles().unwrap();
        assert_eq!(
            roles
                .iter()
                .map(|r| r.name.as_str())
                .collect::<HashSet<_>>(),
            HashSet::from(["admin", "test_user1", "operator"]),
        );
    }
}
