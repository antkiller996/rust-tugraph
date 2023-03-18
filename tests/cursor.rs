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

use std::{collections::HashSet, iter::repeat};

use tugraph::{
    cursor::{EdgeCursor, VertexCursor},
    field::{FieldData, FieldSpec, FieldType},
    txn::{RwTxn, TxnRead, TxnWrite},
};

mod common;

#[test]
fn test_vertex_cursor() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let graph = galaxy.open_graph("default", false).unwrap();
    graph
        .add_vertex_label(
            "Node_0",
            &[FieldSpec {
                name: "id".into(),
                ty: FieldType::Int64,
                optional: false,
            }],
            "id",
        )
        .unwrap();
    graph
        .add_vertex_label(
            "Node_1",
            &[FieldSpec {
                name: "id".into(),
                ty: FieldType::Int64,
                optional: false,
            }],
            "id",
        )
        .unwrap();
    graph
        .add_vertex_label(
            "Node_2",
            &[FieldSpec {
                name: "id".into(),
                ty: FieldType::Int64,
                optional: false,
            }],
            "id",
        )
        .unwrap();
    const NUM_VERTEX: usize = 10;

    {
        let mut rw_txn = graph.create_rw_txn(false).unwrap();
        (0..NUM_VERTEX).for_each(|i| {
            fn add_node(rw_txn: &mut RwTxn<'_>, label: &str, id: i64) {
                rw_txn
                    .add_vertex(label, &["id"], &[FieldData::Int64(id)])
                    .unwrap();
            }
            add_node(&mut rw_txn, "Node_0", i as i64);
            add_node(&mut rw_txn, "Node_1", i as i64);
            add_node(&mut rw_txn, "Node_2", i as i64);
        });
        rw_txn.commit().unwrap();
    }

    {
        let ro_txn = graph.create_ro_txn().unwrap();
        assert_eq!(
            ro_txn.vertex_cur().unwrap().into_vertices().count(),
            NUM_VERTEX * 3
        );
        assert_eq!(
            ro_txn
                .vertex_cur()
                .unwrap()
                .into_vertex_labels()
                .collect::<HashSet<_>>(),
            HashSet::from_iter(["Node_0".into(), "Node_1".into(), "Node_2".into()])
        );
        assert_eq!(
            ro_txn
                .vertex_cur()
                .unwrap()
                .into_vertex_ids()
                .max()
                .unwrap(),
            (NUM_VERTEX * 3 - 1) as i64
        );
        assert_eq!(
            ro_txn
                .vertex_cur()
                .unwrap()
                .into_vertex_fields()
                .map(|fd| {
                    assert_eq!(fd.len(), 1);
                    match fd.first().unwrap() {
                        FieldData::Int64(v) => *v,
                        _ => panic!("fail to convert int64"),
                    }
                })
                .filter(|id| *id == (NUM_VERTEX - 1) as i64)
                .count(),
            3
        );
        let lids: HashSet<_> = ["Node_0", "Node_1", "Node_2"]
            .map(|label| ro_txn.vertex_label_id(label).unwrap() as u16)
            .into();
        assert_eq!(
            ro_txn
                .vertex_cur()
                .unwrap()
                .into_vertex_lids()
                .collect::<HashSet<_>>(),
            lids
        );
    }
}

#[test]
fn test_edge_cursor() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let graph = galaxy.open_graph("default", false).unwrap();
    graph
        .add_vertex_label(
            "Src",
            &[FieldSpec {
                name: "id".into(),
                ty: FieldType::Int64,
                optional: false,
            }],
            "id",
        )
        .unwrap();
    graph
        .add_vertex_label(
            "Dst",
            &[FieldSpec {
                name: "id".into(),
                ty: FieldType::Int64,
                optional: false,
            }],
            "id",
        )
        .unwrap();
    graph
        .add_edge_label(
            "IndexedEdge",
            &[FieldSpec {
                name: "index_field".into(),
                ty: FieldType::Int64,
                optional: false,
            }],
            "",
            [("Src", "Dst")],
        )
        .unwrap();
    graph
        .add_edge_label(
            "TemporalEdge",
            &[FieldSpec {
                name: "tid".into(),
                ty: FieldType::Int64,
                optional: false,
            }],
            "tid",
            [("Src", "Dst")],
        )
        .unwrap();

    graph
        .add_edge_index("IndexedEdge", "index_field", false)
        .unwrap();

    const NUM_INDEXED_EDGE: usize = 5;
    const NUM_TEMPORAL_EDGE: usize = 10;
    let (src, dst) = {
        let mut rw_txn = graph.create_rw_txn(false).unwrap();
        let src = rw_txn
            .add_vertex("Src", &["id"], &[FieldData::Int64(0)])
            .unwrap();
        let dst = rw_txn
            .add_vertex("Dst", &["id"], &[FieldData::Int64(0)])
            .unwrap();

        (0..NUM_INDEXED_EDGE).for_each(|i| {
            rw_txn
                .add_edge(
                    src,
                    dst,
                    "IndexedEdge",
                    &["index_field"],
                    &[FieldData::Int64(i as i64)],
                )
                .unwrap();
        });

        (0..NUM_TEMPORAL_EDGE).for_each(|i| {
            rw_txn
                .add_edge(
                    src,
                    dst,
                    "TemporalEdge",
                    &["tid"],
                    &[FieldData::Int64(i as i64)],
                )
                .unwrap();
        });

        rw_txn.commit().unwrap();
        (src, dst)
    };

    {
        let ro_txn = graph.create_ro_txn().unwrap();

        let mut vcur = ro_txn.vertex_cur().unwrap();
        assert_eq!(
            vcur.seek(src, true)
                .unwrap()
                .out_edge_cursor()
                .unwrap()
                .into_edges()
                .count(),
            vcur.num_out_edges(usize::MAX).unwrap().1
        );

        let mut vcur = ro_txn.vertex_cur().unwrap();
        assert_eq!(
            vcur.seek(dst, true)
                .unwrap()
                .in_edge_cursor()
                .unwrap()
                .into_edges()
                .count(),
            vcur.num_in_edges(usize::MAX).unwrap().1
        );

        let mut vcur = ro_txn.vertex_cur().unwrap();
        let src_cur = vcur.seek(src, true).unwrap();

        assert_eq!(
            src_cur
                .out_edge_cursor()
                .unwrap()
                .into_edge_srcs()
                .collect::<HashSet<_>>(),
            HashSet::from_iter([src])
        );
        assert_eq!(
            src_cur
                .out_edge_cursor()
                .unwrap()
                .into_edge_dsts()
                .collect::<HashSet<_>>(),
            HashSet::from_iter([dst])
        );
        assert_eq!(
            src_cur
                .out_edge_cursor()
                .unwrap()
                .into_edge_labels()
                .collect::<HashSet<_>>(),
            HashSet::from_iter(["TemporalEdge".into(), "IndexedEdge".into()])
        );
        assert_eq!(
            src_cur
                .out_edge_cursor()
                .unwrap()
                .into_edge_tids()
                .collect::<HashSet<_>>(),
            HashSet::from_iter([0].into_iter().chain(0..NUM_TEMPORAL_EDGE as i64))
        );
        assert_eq!(
            src_cur
                .out_edge_cursor()
                .unwrap()
                .into_edge_eids()
                .collect::<Vec<_>>(),
            (0..NUM_INDEXED_EDGE as i64)
                .chain(repeat(0).take(NUM_TEMPORAL_EDGE))
                .collect::<Vec<_>>()
        );

        let index_ecur = ro_txn
            .edge_index_iter_values_from(
                "IndexedEdge",
                "index_field",
                &FieldData::Int64(1),
                &FieldData::Int64(i64::MAX),
            )
            .unwrap();
        assert_eq!(
            index_ecur.collect::<Vec<_>>(),
            (1..NUM_INDEXED_EDGE as i64)
                .map(FieldData::Int64)
                .collect::<Vec<_>>()
        );
    }
}
