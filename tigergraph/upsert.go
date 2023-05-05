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

import (
	"fmt"
)

// UpsertURL defines the tigergraph query endpoint for
// upserting data. It must be appended by the graph name
const UpsertURL = "/graph"

// UpsertResponseResult is the result shape from TigerGraph.
type UpsertResponseResult struct {
	AcceptedVertices     int            `json:"accepted_vertices"`
	AcceptedEdges        int            `json:"accepted_edges"`
	SkippedVertices      int            `json:"skipped_vertices"`
	SkippedEdges         int            `json:"skipped_edges"`
	VerticesAlreadyExist map[string]any `json:"vertices_already_exist"`
	MissVertices         map[string]any `json:"miss_vertices"`
}

// UpsertResponse is the full response from TigerGraph
type UpsertResponse struct {
	Version *Version               `json:"version"`
	Error   bool                   `json:"error"`
	Message string                 `json:"message"`
	Results []UpsertResponseResult `json:"results"`
}

// Upsert upserts data to the given graph.
// https://docs.tigergraph.com/tigergraph-server/current/api/upsert-rest#_examples
func (c *TigerGraphClient) Upsert(graphName string, data any) (*UpsertResponseResult, error) {
	responseResult := &UpsertResponse{}

	err := c.Post(UpsertURL+"/"+graphName, graphName, data, responseResult)

	if err != nil {
		return nil, err
	}

	if responseResult.Error {
		return nil, fmt.Errorf(
			"TigerGraph returned an error when trying to upsert data. Message: %s",
			responseResult.Message,
		)
	}

	return &responseResult.Results[0], nil
}
