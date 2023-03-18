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

use std::collections::HashSet;

use tugraph::{
    cursor::VertexCursor,
    field::{FieldData, FieldSpec, FieldType},
    txn::{TxnRead, TxnWrite},
    types::DateTime,
};

mod common;

#[test]
fn test_search_with_index() {
    let galaxy = common::open_galaxy_in_tmpdir().unwrap();
    let graph = galaxy.open_graph("default", false).unwrap();
    assert!(graph
        .add_vertex_label(
            "Person",
            &[
                FieldSpec {
                    name: "id".into(),
                    ty: FieldType::Int64,
                    optional: false,
                },
                FieldSpec {
                    name: "name".into(),
                    ty: FieldType::String,
                    optional: false,
                },
                FieldSpec {
                    name: "age".into(),
                    ty: FieldType::Int8,
                    optional: false,
                },
            ],
            "id",
        )
        .unwrap());
    assert!(graph
        .add_vertex_label(
            "Comment",
            &[
                FieldSpec {
                    name: "id".into(),
                    ty: FieldType::Int64,
                    optional: false,
                },
                FieldSpec {
                    name: "content".into(),
                    ty: FieldType::String,
                    optional: false,
                },
            ],
            "id",
        )
        .unwrap());
    assert!(graph
        .add_edge_label(
            "Post",
            &[FieldSpec {
                name: "datetime".into(),
                ty: FieldType::DateTime,
                optional: false,
            },],
            "",
            [("Person", "Comment")]
        )
        .unwrap());
    assert!(graph.add_vertex_index("Person", "age", false).unwrap());
    assert!(graph.add_edge_index("Post", "datetime", false).unwrap());

    const NUM_ALICE_POSTS: usize = 5;
    {
        let mut rw_txn = graph.create_rw_txn(false).unwrap();
        let _bob = rw_txn
            .add_vertex(
                "Person",
                &["id", "name", "age"],
                &[
                    FieldData::Int64(0),
                    FieldData::String("Bob".into()),
                    FieldData::Int8(18),
                ],
            )
            .unwrap();

        let _jack = rw_txn
            .add_vertex(
                "Person",
                &["id", "name", "age"],
                &[
                    FieldData::Int64(1),
                    FieldData::String("Jack".into()),
                    FieldData::Int8(21),
                ],
            )
            .unwrap();

        let alice = rw_txn
            .add_vertex(
                "Person",
                &["id", "name", "age"],
                &[
                    FieldData::Int64(2),
                    FieldData::String("Alice".into()),
                    FieldData::Int8(17),
                ],
            )
            .unwrap();
        (0..NUM_ALICE_POSTS).for_each(|i| {
            let comment = rw_txn
                .add_vertex(
                    "Comment",
                    &["id", "content"],
                    &[
                        FieldData::Int64(i as i64),
                        FieldData::String(format!("Alice's Comment {i}")),
                    ],
                )
                .unwrap();
            rw_txn
                .add_edge(
                    alice,
                    comment,
                    "Post",
                    &["datetime"],
                    &[FieldData::DateTime(
                        DateTime::from_timestamp_opt(1000 + i as i64).unwrap(),
                    )],
                )
                .unwrap();
        });
        rw_txn.commit().unwrap();
    }

    {
        let ro_txn = graph.create_ro_txn().unwrap();
        assert!(ro_txn.is_vertex_indexed("Person", "age").unwrap());
        assert!(!ro_txn.is_vertex_indexed("Comment", "content").unwrap());
        assert!(ro_txn.is_edge_indexed("Post", "datetime").unwrap());

        let v_indexes = ro_txn.all_vertex_indexes().unwrap();
        // Person.id, Person.age, Comment.id
        assert_eq!(v_indexes.len(), 3);
        assert_eq!(
            v_indexes
                .iter()
                .map(|i| format!("{}.{}", i.label, i.field))
                .collect::<HashSet<_>>(),
            HashSet::from_iter(["Person.id".into(), "Person.age".into(), "Comment.id".into()])
        );

        let e_indexes = ro_txn.all_edge_indexes().unwrap();
        assert_eq!(e_indexes.len(), 1);
        assert_eq!(
            e_indexes
                .iter()
                .map(|i| format!("{}.{}", i.label, i.field))
                .collect::<HashSet<_>>(),
            HashSet::from_iter(["Post.datetime".into()])
        );

        let ages: Vec<_> = ro_txn
            .vertex_index_iter_values_from(
                "Person",
                "age",
                &FieldData::Int8(18),
                &FieldData::Int8(20),
            )
            .unwrap()
            .collect();
        assert_eq!(ages, vec![FieldData::Int8(18)]);

        let post_times: Vec<_> = ro_txn
            .edge_index_iter_values_from(
                "Post",
                "datetime",
                &FieldData::DateTime(DateTime::from_timestamp_opt(1001).unwrap()),
                &FieldData::DateTime(DateTime::from_timestamp_opt(2000).unwrap()),
            )
            .unwrap()
            .collect();
        assert_eq!(
            post_times,
            (0..NUM_ALICE_POSTS)
                .map(|i| 1000 + i)
                .filter(|i| (1001..2000).contains(i))
                .map(|datetime| {
                    FieldData::DateTime(DateTime::from_timestamp_opt(datetime as i64).unwrap())
                })
                .collect::<Vec<_>>()
        );

        let primary_vertex_index = ro_txn
            .unique_index_vertex_cur("Person", "id", &FieldData::Int64(1))
            .unwrap();
        assert!(primary_vertex_index.is_valid());
        assert_eq!(
            primary_vertex_index.field("name").unwrap(),
            FieldData::String("Jack".into())
        );

        let person_lid = ro_txn.vertex_label_id("Person").unwrap();
        let person_id_fid = ro_txn.vertex_field_id(person_lid, "id").unwrap();
        let primary_vertex_index = ro_txn
            .unique_index_vertex_cur_by_id(person_lid, person_id_fid, &FieldData::Int64(1))
            .unwrap();
        assert!(primary_vertex_index.is_valid());
        assert_eq!(
            primary_vertex_index.field("name").unwrap(),
            FieldData::String("Jack".into())
        );
    }
}
