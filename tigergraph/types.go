/*
Copyright 2023 Adarga Limited

Licensed under the Apache License, Version 2.0 (the "License"). You may not use
this file except in compliance with the License. You may obtain a copy of the
License at:
https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
*/
package tigergraph

type Version struct {
	Edition string `json:"edition"`
	API     string `json:"api"`
	Schema  int    `json:"schema"`
}

type Vertex struct {
	ID string `json:"id"`
}

type ResponseVertex[T any] struct {
	VID        string `json:"v_id"`
	VType      string `json:"v_type"`
	Attributes T      `json:"attributes"`
}

type TigerGraphResponse[T any] struct {
	Version Version `json:"version"`
	Message string  `json:"message"`
	Error   bool    `json:"error"`
	Results []T     `json:"results"`
}
