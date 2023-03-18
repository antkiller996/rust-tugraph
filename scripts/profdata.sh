# Copyright 2023 antkiller
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env bash
SCRIPTS_DIR=$(dirname "$0")

rm default_*.profraw rust_tugraph.profdata 2> /dev/null
rm -rf /tmp/rust_tugraph 2> /dev/null
cargo clean \
&& RUSTFLAGS="-C instrument-coverage" \
LD_LIBRARY_PATH="$SCRIPTS_DIR/../libtugraph-sys/tugraph-db/build/output" \
    cargo test -p rust-tugraph
cargo profdata -- merge -sparse default_*.profraw -o rust_tugraph.profdata