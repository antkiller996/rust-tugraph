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

use proc_macro::{TokenStream, TokenTree::Ident};
use quote::quote;

/// A helper attribute macro brings easy life to write tugraph rust procedure
#[proc_macro_attribute]
pub fn tugraph_plugin(_attr: TokenStream, input: TokenStream) -> TokenStream {
    // skip the first identifier(keyword) `fn`
    // the second identifier is name of function.
    let func_name = input
        .clone()
        .into_iter()
        .find_map(|tt| match tt {
            Ident(ref name) if name.to_string() != "fn" && name.to_string() != "pub" => Some(tt),
            _ => None,
        })
        .unwrap();
    let func_name = proc_macro2::TokenStream::from(TokenStream::from(func_name));
    let user_process_func = proc_macro2::TokenStream::from(input);
    let extern_c_process = quote! {
        use plugin_util::lgraph_api_graph_db_t;
        use plugin_util::CxxString;
        use plugin_util::Graph as TuGraph;
        #[allow(clippy::missing_safety_doc)]
        #[no_mangle]
        pub unsafe extern "C" fn Process(graph_db: *mut lgraph_api_graph_db_t, request: *const CxxString, response: *mut CxxString) -> bool {
            let mut graph = TuGraph::from_ptr(graph_db);
            let request = if request.is_null() {
                ""
            } else {
                (*request).to_str().unwrap()
            };
            // nest user defined process function
            #user_process_func

            let result = #func_name(&mut graph, request);
            ::std::mem::forget(graph);

            match result {
                Ok(val) => {
                    if !response.is_null() {
                        let mut reponse = ::std::pin::Pin::new_unchecked(&mut *response);
                        reponse.as_mut().clear();
                        reponse.as_mut().push_str(val.as_str())
                    }
                    true
                }
                Err(e) => {
                    eprintln!("run rust plugin failed: {:?}", e);
                    false
                }
            }
        }
    };
    TokenStream::from(extern_c_process)
}
