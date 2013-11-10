/* Copyright 2013 Leon Sixt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



#[license = "MIT/ASL2"];

#[crate_type = "lib"];
#[feature(macro_rules)];
#[link(name = "libtrees", vers = "0.1", package_id = "libtrees")];


extern mod std;
extern mod extra;


mod algorithm;
mod node;
mod blinktree {
    mod node;
    mod blinktree;
}
mod lock;
mod storage;
mod statistics;
mod utils;
