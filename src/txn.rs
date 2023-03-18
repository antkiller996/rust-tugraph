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

//! TuGraph operations happen in transactions. A transaction is sequence of operations
//! that is carried out atomically on the [`Graph`]. TuGraph transactions provides full
//! ACID guarantees.
//!          
//! Transactions are created using [`Graph::create_ro_txn`] and
//! [`Graph::create_rw_txn`]. A read transaction can only perform read operations,
//! otherwise an exception is thrown. A write transaction can perform reads as well as
//! writes. There are performance differences between read and write operations. So if
//! you only need read in a transaction, you should create a read transaction.
//!
//! Each transaction must be used in one thread only, and they should not be sent across
//! threads unless it is a forked transaction.
//!
//! Read-only(for short, read) transactions can be forked. The new copy of the transaction will have the same
//! view as the forked one, and it can be used in a separate thread. By forking from one
//! read transaction and using the forked copies in different threads, we can parallelize
//! the execution of specific operations. For example, you can implement a parallel BFS
//! with this capability. Also, you can dump a snapshot of the whole graph using the forked
//! one.
//!
//! [`Graph`]: crate::db::Graph
//! [`Graph::create_ro_txn`]: crate::db::Graph::create_ro_txn
//! [`Graph::create_rw_txn`]: crate::db::Graph::create_rw_txn

use std::{fmt::Debug, marker::PhantomData};

use crate::{
    cursor::{
        EdgeIndexDsts, EdgeIndexEdgeIds, EdgeIndexIter, EdgeIndexLabelIds, EdgeIndexSrcs,
        EdgeIndexUids, EdgeIndexValues, OutEdgeCur, OutEdgeCurMut, VertexCur, VertexCurMut,
        VertexIndexIds, VertexIndexIter, VertexIndexValues,
    },
    field::{FieldData, FieldSpec},
    index::IndexSpec,
    raw::RawTransaction,
    types::EdgeUid,
    Result,
};

trait AsRawTransaction {
    fn as_raw(&self) -> &RawTransaction;
}

/// `TxnRead` trait provides all read operations of a transaction.
///
/// You can use `TxnRead` to get vertex cursor which can move back and forth
/// to read graph vertices. Also, you can query schema, label of vertex and edge.
pub trait TxnRead {
    /// Query if this transaction is valid. Transaction becomes invalid after dropped
    // or committed. Operations on invalid transaction return errors.
    fn is_valid(&self) -> bool;

    /// Query if this txn is read only.
    fn is_read_only(&self) -> bool;

    /// Get a read-only vertex cursor pointing to the first vertex. If there is no vertex, the
    /// cursor is invalid.
    fn vertex_cur(&self) -> Result<VertexCur<'_>>;

    /// Get number of vertex labels
    fn num_vertex_labels(&self) -> Result<usize>;

    /// Get number of edge labels.
    fn num_edge_labels(&self) -> Result<usize>;

    /// List all vertex labels.
    fn all_vertex_labels(&self) -> Result<Vec<String>>;

    /// List all edge labels.
    fn all_edge_labels(&self) -> Result<Vec<String>>;

    /// Get vertex label id corresponding to the label name.
    fn vertex_label_id(&self, label: &str) -> Result<usize>;

    /// Get edge label id corresponding to the label name.
    fn edge_label_id(&self, label: &str) -> Result<usize>;

    /// Get vertex schema definition corresponding to the vertex label.
    fn vertex_schema(&self, label: &str) -> Result<Vec<FieldSpec>>;

    /// Get edge schema definition corresponding to the edge label.
    fn edge_schema(&self, label: &str) -> Result<Vec<FieldSpec>>;

    /// Get vertex field id.
    fn vertex_field_id(&self, label_id: usize, field_name: &str) -> Result<usize>;

    /// Get vertex field ids.
    fn vertex_fields_ids<'a, T>(&self, label_id: usize, field_names: T) -> Result<Vec<usize>>
    where
        T: IntoIterator<Item = &'a str>;

    /// Get edge field id.
    fn edge_field_id(&self, label_id: usize, field_name: &str) -> Result<usize>;

    /// Get edge field ids.
    fn edge_fields_ids<'a, T>(&self, label_id: usize, field_names: T) -> Result<Vec<usize>>
    where
        T: IntoIterator<Item = &'a str>;

    /// Query if vertex index is ready for use. This should be used only to decide whether to
    /// use an index.
    ///        
    /// Building index of vertex  is in background, especially when added for a (label, field) that
    /// already has a lot of vertices. This function tells us if the index building is
    /// finished.
    ///        
    /// **NOTE**: DO NOT wait for index building in a write transaction. Write transactions block other
    /// write transactions, so blocking in a write transaction is always a bad idea. And
    /// long-living read transactions interfere with GC, making the DB grow unexpectly.
    fn is_vertex_indexed(&self, label: &str, field: &str) -> Result<bool>;

    /// Query if edge index is ready for use.
    ///
    /// See the [`TxnRead::is_vertex_indexed`] for more details.
    fn is_edge_indexed(&self, label: &str, field: &str) -> Result<bool>;

    /// List all vertex indexes
    fn all_vertex_indexes(&self) -> Result<Vec<IndexSpec>>;

    /// List all edge indexes
    fn all_edge_indexes(&self) -> Result<Vec<IndexSpec>>;

    /// Get vertex index iterator.
    ///
    /// The iterator has field value range [key_start, key_end]. So
    /// key_start=key_end=v returns an iterator pointing to all vertexes that has field
    /// value v.
    fn vertex_index_iter_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexIter<'_>>;

    /// Get vertex index iterator over id.
    ///
    /// An iterator adaptor of [`TxnRead::vertex_index_iter_from`] which returns
    /// a iterator over id instead of (`id`, `Vec<FieldData>`) pair.
    fn vertex_index_iter_ids_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexIds<'_>>;

    /// Get vertex index iterator of field values.
    ///
    /// An iterator adaptor of [`TxnRead::vertex_index_iter_from`] which returns
    /// a iterator over `Vec<FieldData>` instead of (`id`, `Vec<FieldData>`) pair.
    fn vertex_index_iter_values_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexValues<'_>>;

    /// Get vertex index iterator.
    ///
    /// An overloaded one of [`TxnRead::vertex_index_iter_from_by_id`].
    fn vertex_index_iter_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexIter<'_>>;

    /// Get vertex index iterator over id.
    ///
    /// An overloaded one of [`TxnRead::vertex_index_iter_ids_from`].
    fn vertex_index_iter_ids_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexIds<'_>>;

    /// Get vertex index iterator of field values.
    ///
    /// An overloaded one of [`TxnRead::vertex_index_iter_values_from`].
    fn vertex_index_iter_values_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexValues<'_>>;

    /// Get edge index iterator.
    ///
    /// The iterator has field value range [key_start, key_end]. So
    /// key_start=key_end=v returns an iterator pointing to all edges
    /// that has field value v.
    fn edge_index_iter_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexIter<'_>>;

    /// Get edge index iterator over [`EdgeUid`].
    ///
    /// A iterator adaptor of [`TxnRead::edge_index_iter_from`] which returns
    /// a iterator over EdgeUid instead of (`EdgeUid`, `Vec<FieldData>`) pair.
    fn edge_index_iter_uids_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexUids<'_>>;

    /// Get edge index iterator over src vertex id of edge.
    ///
    /// A iterator adaptor of [`TxnRead::edge_index_iter_from`] which returns
    /// a iterator over src vertex id of edge instead of (`EdgeUid`, `Vec<FieldData>`) pair.
    fn edge_index_iter_srcs_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexSrcs<'_>>;

    /// Get edge index iterator over dst vertex id of edge.
    ///
    /// A iterator adaptor of [`TxnRead::edge_index_iter_from`] which returns
    /// a iterator over dst vertex id of edge instead of (`EdgeUid`, `Vec<FieldData>`) pair.
    fn edge_index_iter_dsts_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexDsts<'_>>;

    /// Get edge index iterator over label id of edge.
    ///
    /// A iterator adaptor of [`TxnRead::edge_index_iter_from`] which returns
    /// a iterator over label id of edge instead of (`EdgeUid`, `Vec<FieldData>`) pair.
    fn edge_index_iter_lids_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexLabelIds<'_>>;

    /// Get edge index iterator over edge id.
    ///
    /// A iterator adaptor of [`TxnRead::edge_index_iter_from`] which returns
    /// a iterator over edge id instead of (`EdgeUid`, `Vec<FieldData>`) pair.
    fn edge_index_iter_eids_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexEdgeIds<'_>>;

    /// Get edge index iterator over field values.
    ///
    /// A iterator adaptor of [`TxnRead::edge_index_iter_from`] which returns
    /// a iterator over field values id instead of (`EdgeUid`, `Vec<FieldData>`) pair.
    fn edge_index_iter_values_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexValues<'_>>;

    /// Get edge index iterator.
    ///
    /// An overloaded one of [`TxnRead::edge_index_iter_from`].
    fn edge_index_iter_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexIter<'_>>;

    /// Get edge index iterator over src vertex id of edge.
    ///
    /// An overloaded one of [`TxnRead::edge_index_iter_from`].
    fn edge_index_iter_srcs_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexSrcs<'_>>;

    /// Get edge index iterator over dst vertex id of edge.
    ///
    /// An overloaded one of [`TxnRead::edge_index_iter_dsts_from`].
    fn edge_index_iter_dsts_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexDsts<'_>>;

    /// Get edge index iterator over label id of edge.
    ///
    /// An overloaded one of [`TxnRead::edge_index_iter_lids_from`].
    fn edge_index_iter_lids_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexLabelIds<'_>>;

    /// Get edge index iterator over edge id.
    ///
    /// An overloaded one of [`TxnRead::edge_index_iter_eids_from`].
    fn edge_index_iter_eids_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexEdgeIds<'_>>;

    /// Get edge index iterator over field value.
    ///
    /// An overloaded one of [`TxnRead::edge_index_iter_values_from`].
    fn edge_index_iter_values_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexValues<'_>>;

    /// Get vertex cursor by unique index.
    fn unique_index_vertex_cur(
        &self,
        label: &str,
        field: &str,
        value: &FieldData,
    ) -> Result<VertexCur<'_>>;

    /// Get vertex cursor by unique index.
    ///
    /// An overloaded one of [`TxnRead::unique_index_vertex_cur`]
    fn unique_index_vertex_cur_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        field_value: &FieldData,
    ) -> Result<VertexCur<'_>>;

    /// Get out edge cursor by unique index.
    fn unique_index_out_edgr_cur(
        &self,
        label: &str,
        field: &str,
        value: &FieldData,
    ) -> Result<OutEdgeCur<'_>>;

    /// Get out edge cursor by unique index.
    ///
    /// An overloaded one of [`TxnRead::unique_index_out_edgr_cur`]
    fn unique_index_out_edgr_cur_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        value: &FieldData,
    ) -> Result<OutEdgeCur<'_>>;

    /// Get the number of vertices.
    fn num_vertices(&self) -> Result<usize>;

    /// Get vertex primary field
    fn get_vertex_primary_field(&self, label: &str) -> Result<String>;
}

impl<T> TxnRead for T
where
    T: AsRawTransaction,
{
    fn is_valid(&self) -> bool {
        self.as_raw().is_valid()
    }

    fn is_read_only(&self) -> bool {
        self.as_raw().is_read_only()
    }

    fn vertex_cur(&self) -> Result<VertexCur<'_>> {
        self.as_raw().get_vertex_iterator().map(VertexCur::new)
    }

    fn num_vertex_labels(&self) -> Result<usize> {
        self.as_raw().get_num_vertex_labels()
    }

    fn num_edge_labels(&self) -> Result<usize> {
        self.as_raw().get_num_edge_labels()
    }

    fn all_vertex_labels(&self) -> Result<Vec<String>> {
        self.as_raw().list_vertex_labels()
    }

    fn all_edge_labels(&self) -> Result<Vec<String>> {
        self.as_raw().list_edge_labels()
    }

    fn vertex_label_id(&self, label: &str) -> Result<usize> {
        self.as_raw().get_vertex_label_id(label)
    }

    fn edge_label_id(&self, label: &str) -> Result<usize> {
        self.as_raw().get_edge_label_id(label)
    }

    fn vertex_schema(&self, label: &str) -> Result<Vec<FieldSpec>> {
        self.as_raw().get_vertex_schema(label).map(|fss| {
            fss.into_iter()
                .map(|raw| FieldSpec::from_raw_field_spec(&raw))
                .collect()
        })
    }

    fn edge_schema(&self, label: &str) -> Result<Vec<FieldSpec>> {
        self.as_raw().get_edge_schema(label).map(|fss| {
            fss.into_iter()
                .map(|raw| FieldSpec::from_raw_field_spec(&raw))
                .collect()
        })
    }

    fn vertex_field_id(&self, label_id: usize, field_name: &str) -> Result<usize> {
        self.as_raw().get_vertex_field_id(label_id, field_name)
    }

    fn vertex_fields_ids<'a, N>(&self, label_id: usize, field_names: N) -> Result<Vec<usize>>
    where
        N: IntoIterator<Item = &'a str>,
    {
        self.as_raw().get_vertex_field_ids(label_id, field_names)
    }

    fn edge_field_id(&self, label_id: usize, field_name: &str) -> Result<usize> {
        self.as_raw().get_edge_field_id(label_id, field_name)
    }

    fn edge_fields_ids<'a, N>(&self, label_id: usize, field_names: N) -> Result<Vec<usize>>
    where
        N: IntoIterator<Item = &'a str>,
    {
        self.as_raw().get_edge_field_ids(label_id, field_names)
    }

    fn is_vertex_indexed(&self, label: &str, field: &str) -> Result<bool> {
        self.as_raw().is_vertex_indexed(label, field)
    }

    fn is_edge_indexed(&self, label: &str, field: &str) -> Result<bool> {
        self.as_raw().is_edge_indexed(label, field)
    }

    fn all_vertex_indexes(&self) -> Result<Vec<IndexSpec>> {
        self.as_raw().list_vertex_indexes().map(|iss| {
            iss.into_iter()
                .map(|is| IndexSpec::from_raw_index_spec(&is))
                .collect()
        })
    }

    fn all_edge_indexes(&self) -> Result<Vec<IndexSpec>> {
        self.as_raw().list_edge_indexes().map(|iss| {
            iss.into_iter()
                .map(|is| IndexSpec::from_raw_index_spec(&is))
                .collect()
        })
    }

    fn vertex_index_iter_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexIter<'_>> {
        self.as_raw()
            .get_vertex_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(VertexIndexIter::new)
    }

    fn vertex_index_iter_ids_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexIds<'_>> {
        self.as_raw()
            .get_vertex_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(VertexIndexIds::new)
    }

    fn vertex_index_iter_values_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexValues<'_>> {
        self.as_raw()
            .get_vertex_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(VertexIndexValues::new)
    }

    fn vertex_index_iter_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexIter<'_>> {
        self.as_raw()
            .get_vertex_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(VertexIndexIter::new)
    }

    fn vertex_index_iter_ids_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexIds<'_>> {
        self.as_raw()
            .get_vertex_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(VertexIndexIds::new)
    }

    fn vertex_index_iter_values_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<VertexIndexValues<'_>> {
        self.as_raw()
            .get_vertex_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(VertexIndexValues::new)
    }

    fn edge_index_iter_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexIter<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexIter::new)
    }

    fn edge_index_iter_uids_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexUids<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexUids::new)
    }

    fn edge_index_iter_srcs_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexSrcs<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexSrcs::new)
    }

    fn edge_index_iter_dsts_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexDsts<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexDsts::new)
    }

    fn edge_index_iter_lids_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexLabelIds<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexLabelIds::new)
    }

    fn edge_index_iter_eids_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexEdgeIds<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexEdgeIds::new)
    }

    fn edge_index_iter_values_from(
        &self,
        label: &str,
        field: &str,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexValues<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_data(
                label,
                field,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexValues::new)
    }

    fn edge_index_iter_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexIter<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexIter::new)
    }

    fn edge_index_iter_srcs_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexSrcs<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexSrcs::new)
    }

    fn edge_index_iter_dsts_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexDsts<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexDsts::new)
    }

    fn edge_index_iter_lids_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexLabelIds<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexLabelIds::new)
    }

    fn edge_index_iter_eids_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexEdgeIds<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexEdgeIds::new)
    }

    fn edge_index_iter_values_from_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        start: &FieldData,
        end: &FieldData,
    ) -> Result<EdgeIndexValues<'_>> {
        self.as_raw()
            .get_edge_index_iterator_by_id(
                label_id,
                field_id,
                &start.as_raw_field_data(),
                &end.as_raw_field_data(),
            )
            .map(EdgeIndexValues::new)
    }

    fn unique_index_vertex_cur(
        &self,
        label: &str,
        field: &str,
        value: &FieldData,
    ) -> Result<VertexCur<'_>> {
        self.as_raw()
            .get_vertex_by_unique_index_by_data(label, field, &value.as_raw_field_data())
            .map(VertexCur::new)
    }

    fn unique_index_vertex_cur_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        field_value: &FieldData,
    ) -> Result<VertexCur<'_>> {
        self.as_raw()
            .get_vertex_by_unique_index_id(label_id, field_id, &field_value.as_raw_field_data())
            .map(VertexCur::new)
    }

    fn unique_index_out_edgr_cur(
        &self,
        label: &str,
        field: &str,
        value: &FieldData,
    ) -> Result<OutEdgeCur<'_>> {
        self.as_raw()
            .get_edge_by_unique_index_by_data(label, field, &value.as_raw_field_data())
            .map(OutEdgeCur::new)
    }

    fn unique_index_out_edgr_cur_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        value: &FieldData,
    ) -> Result<OutEdgeCur<'_>> {
        self.as_raw()
            .get_edge_by_unique_index_id(label_id, field_id, &value.as_raw_field_data())
            .map(OutEdgeCur::new)
    }

    fn num_vertices(&self) -> Result<usize> {
        self.as_raw().get_num_vertices()
    }

    fn get_vertex_primary_field(&self, label: &str) -> Result<String> {
        self.as_raw().get_vertex_primary_field(label)
    }
}

/// `TxnWrite` trait provides all write operations of a transaction.
///
/// You can use `TxnWrite` to get vertex cursor which can move back and forth
/// to modify graph vertices.
pub trait TxnWrite {
    /// Get a vertex cursor that allowd modifying each vertex.
    fn vertex_cur_mut(&self) -> Result<VertexCurMut<'_>>;

    /// Get a vertex cursor by unique index that allowd modifying each vertex.
    fn unique_index_vertex_cur_mut(
        &self,
        label: &str,
        field: &str,
        value: &FieldData,
    ) -> Result<VertexCurMut<'_>>;

    /// Get a vertex cursor by unique index that allowd modifying each vertex.
    fn unique_index_vertex_cur_mut_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        field_value: &FieldData,
    ) -> Result<VertexCurMut<'_>>;

    /// Get a out edge cursor by unique index that allow modifying each edge.
    fn unique_index_out_edgr_cur_mut(
        &self,
        label: &str,
        field: &str,
        value: &FieldData,
    ) -> Result<OutEdgeCurMut<'_>>;

    /// Get a out edge cursor by unique index that allow modifying each edge.
    ///
    /// An overloaded one of [`TxnWrite::unique_index_out_edgr_cur_mut`]
    fn unique_index_out_edgr_cur_mut_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        value: &FieldData,
    ) -> Result<OutEdgeCurMut<'_>>;

    /// Add a vertex.
    ///
    /// All non-nullable fields must be specified. All indexes of vertex are also
    /// updated.
    ///
    /// # Errors
    /// If a unique field is indexed for the vertex, and the same field value exists,
    /// an error is retured.
    fn add_vertex<'a, T>(
        &mut self,
        label: &str,
        field_names: &[&str],
        field_values: T,
    ) -> Result<i64>
    where
        T: IntoIterator<Item = &'a FieldData>;

    /// Add a vertex.
    ///
    /// An overloaded one of [`TxnWrite::add_vertex`].
    fn add_vertex_by_id<'a, 'b, T>(
        &mut self,
        label_id: usize,
        field_ids: &[usize],
        field_values: T,
    ) -> Result<i64>
    where
        T: IntoIterator<Item = &'b FieldData>;

    /// Add a edge.
    ///
    /// All non-nullable fields must be specified. All indexes of edge are also
    /// updated.
    ///
    /// # Errors
    /// If a unique field is indexed for the edge, and the same field value exists,
    /// an error is retured.
    fn add_edge<'a, T>(
        &mut self,
        src: i64,
        dst: i64,
        label: &str,
        field_names: &[&str],
        field_values: T,
    ) -> Result<EdgeUid>
    where
        T: IntoIterator<Item = &'a FieldData>;

    /// Add a edge.
    ///
    /// An overloaded one of [`TxnWrite::add_edge`].
    fn add_edge_by_id<'a, T>(
        &mut self,
        src: i64,
        dst: i64,
        label_id: usize,
        field_ids: &[usize],
        field_values: T,
    ) -> Result<EdgeUid>
    where
        T: IntoIterator<Item = &'a FieldData>;

    /// Upsert edge.
    ///
    /// If there is no src->dst edge, insert it. Otherwise, try to update
    /// the edge's property.
    ///
    /// **Note**: if edge id is used in primary key, only the edge whose edge id == 0
    /// will be updated.
    ///
    /// # Errors
    /// If the edge exists and the label differs from specified
    /// label, an error is returned.
    fn upsert_edge<'a, T>(
        &mut self,
        src: i64,
        dst: i64,
        label: &str,
        field_names: &[&str],
        field_values: T,
    ) -> Result<bool>
    where
        T: IntoIterator<Item = &'a FieldData>;

    /// Upsert edge.
    ///
    /// An overloaded one of [`TxnWrite::upsert_edge`]
    fn upsert_edge_by_id<'a, T>(
        &mut self,
        src: i64,
        dst: i64,
        label_id: usize,
        field_ids: &[usize],
        field_values: T,
    ) -> Result<bool>
    where
        T: IntoIterator<Item = &'a FieldData>;
}

pub struct RoTxn<'g> {
    inner: RawTransaction,
    // the underlying ffi transaction of `RawTransaction` has a reference
    // to ffi graph db
    _graph: PhantomData<&'g ()>,
}

impl<'g> Debug for RoTxn<'g> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RoTxn").finish()
    }
}

impl<'g> RoTxn<'g> {
    pub(crate) fn from_raw(raw: RawTransaction) -> Self {
        RoTxn {
            inner: raw,
            _graph: PhantomData,
        }
    }
    pub(crate) fn as_raw(&self) -> &RawTransaction {
        &self.inner
    }
}

impl<'g> AsRawTransaction for RoTxn<'g> {
    fn as_raw(&self) -> &RawTransaction {
        &self.inner
    }
}

unsafe impl Sync for RoTxn<'_> {}

impl<'g> TxnWrite for RwTxn<'g> {
    fn vertex_cur_mut(&self) -> Result<VertexCurMut<'_>> {
        self.as_raw().get_vertex_iterator().map(VertexCurMut::new)
    }

    fn unique_index_vertex_cur_mut(
        &self,
        label: &str,
        field: &str,
        value: &FieldData,
    ) -> Result<VertexCurMut<'_>> {
        self.as_raw()
            .get_vertex_by_unique_index_by_data(label, field, &value.as_raw_field_data())
            .map(VertexCurMut::new)
    }

    fn unique_index_vertex_cur_mut_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        field_value: &FieldData,
    ) -> Result<VertexCurMut<'_>> {
        self.as_raw()
            .get_vertex_by_unique_index_id(label_id, field_id, &field_value.as_raw_field_data())
            .map(VertexCurMut::new)
    }

    fn unique_index_out_edgr_cur_mut(
        &self,
        label: &str,
        field: &str,
        value: &FieldData,
    ) -> Result<OutEdgeCurMut<'_>> {
        self.as_raw()
            .get_edge_by_unique_index_by_data(label, field, &value.as_raw_field_data())
            .map(OutEdgeCurMut::new)
    }

    fn unique_index_out_edgr_cur_mut_by_id(
        &self,
        label_id: usize,
        field_id: usize,
        value: &FieldData,
    ) -> Result<OutEdgeCurMut<'_>> {
        self.as_raw()
            .get_edge_by_unique_index_id(label_id, field_id, &value.as_raw_field_data())
            .map(OutEdgeCurMut::new)
    }

    fn add_vertex<'a, V>(
        &mut self,
        label: &str,
        field_names: &[&str],
        field_values: V,
    ) -> Result<i64>
    where
        V: IntoIterator<Item = &'a FieldData>,
    {
        let raw_field_values: Vec<_> = field_values
            .into_iter()
            .map(|fd| fd.as_raw_field_data())
            .collect();
        self.as_raw()
            .add_vertex_by_data(label, field_names.iter().copied(), &raw_field_values)
    }
    fn add_vertex_by_id<'a, 'b, V>(
        &mut self,
        label_id: usize,
        field_ids: &[usize],
        field_values: V,
    ) -> Result<i64>
    where
        V: IntoIterator<Item = &'b FieldData>,
    {
        let raw_field_values: Vec<_> = field_values
            .into_iter()
            .map(|fd| fd.as_raw_field_data())
            .collect();
        self.as_raw()
            .add_vertex_by_ids(label_id, field_ids, &raw_field_values)
    }

    fn add_edge<'a, V>(
        &mut self,
        src: i64,
        dst: i64,
        label: &str,
        field_names: &[&str],
        field_values: V,
    ) -> Result<EdgeUid>
    where
        V: IntoIterator<Item = &'a FieldData>,
    {
        let raw_field_values: Vec<_> = field_values
            .into_iter()
            .map(|fd| fd.as_raw_field_data())
            .collect();
        self.as_raw()
            .add_edge_by_data(
                src,
                dst,
                label,
                field_names.iter().copied(),
                &raw_field_values,
            )
            .map(|raw| EdgeUid::from_raw(&raw))
    }
    fn add_edge_by_id<'a, V>(
        &mut self,
        src: i64,
        dst: i64,
        label_id: usize,
        field_ids: &[usize],
        field_values: V,
    ) -> Result<EdgeUid>
    where
        V: IntoIterator<Item = &'a FieldData>,
    {
        let raw_field_values: Vec<_> = field_values
            .into_iter()
            .map(|fd| fd.as_raw_field_data())
            .collect();
        self.as_raw()
            .add_edge_by_id(src, dst, label_id, field_ids, &raw_field_values)
            .map(|raw| EdgeUid::from_raw(&raw))
    }

    fn upsert_edge<'a, V>(
        &mut self,
        src: i64,
        dst: i64,
        label: &str,
        field_names: &[&str],
        field_values: V,
    ) -> Result<bool>
    where
        V: IntoIterator<Item = &'a FieldData>,
    {
        let raw_field_values: Vec<_> = field_values
            .into_iter()
            .map(|fd| fd.as_raw_field_data())
            .collect();
        self.as_raw().upsert_edge_by_data(
            src,
            dst,
            label,
            field_names.iter().copied(),
            &raw_field_values,
        )
    }

    fn upsert_edge_by_id<'a, V>(
        &mut self,
        src: i64,
        dst: i64,
        label_id: usize,
        field_ids: &[usize],
        field_values: V,
    ) -> Result<bool>
    where
        V: IntoIterator<Item = &'a FieldData>,
    {
        let raw_field_values: Vec<_> = field_values
            .into_iter()
            .map(|fd| fd.as_raw_field_data())
            .collect();
        self.as_raw()
            .upsert_edge_by_id(src, dst, label_id, field_ids, &raw_field_values)
    }
}

pub struct RwTxn<'g> {
    inner: RawTransaction,
    // the underlying ffi transaction of `RawTransaction` has a reference
    // to ffi graph db
    _graph: PhantomData<&'g ()>,
}

impl<'g> RwTxn<'g> {
    pub(crate) fn from_raw(raw: RawTransaction) -> Self {
        RwTxn {
            inner: raw,
            _graph: PhantomData,
        }
    }
}

impl<'g> AsRawTransaction for RwTxn<'g> {
    fn as_raw(&self) -> &RawTransaction {
        &self.inner
    }
}

impl<'g> Debug for RwTxn<'g> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RwTxn").finish()
    }
}

impl<'g> RwTxn<'g> {
    /// Commit transaction.
    ///
    /// **Note**: A optimistic write transactions may fail to commit (an TxnConflictError would be thrown).
    /// # Errors
    /// If a transaction conflicts with an ealier one, a [`ErrorKind::TxnConflict`] will be returned during commit.
    ///
    /// [`ErrorKind::TxnConflict`]: crate::ErrorKind::TxnConflict
    pub fn commit(self) -> Result<()> {
        self.inner.commit()
    }
}
