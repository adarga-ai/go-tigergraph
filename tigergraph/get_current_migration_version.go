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

import "context"

// GetCurrentMigrationVersionURL is the URL to get the current migration version
const GetCurrentMigrationVersionURL = "/query/get_latest_migration"

// MigrationVertexAttributes is the attributes of a migration vertex
type MigrationVertexAttributes struct {
	CreatedAt       string `json:"created_at"`
	MigrationNumber string `json:"migration_number"`
	Mode            string `json:"mode"`
	GraphName       string `json:"graph_name"`
}

// MigrationVertex is the shape of a returned migration vertex
type MigrationVertex struct {
	Attributes MigrationVertexAttributes `json:"attributes"`
	VID        string                    `json:"v_id"`
	VType      string                    `json:"v_type"`
}

// CurrentMigrationVersionResponseResult is the result shape inside the response
type CurrentMigrationVersionResponseResult struct {
	LatestMigration []MigrationVertex `json:"latest_migration"`
}

// CurrentMigrationVersionResponse is the response from TG containing the migration version
type CurrentMigrationVersionResponse struct {
	Version *Version                                `json:"version"`
	Error   bool                                    `json:"error"`
	Message string                                  `json:"message"`
	Results []CurrentMigrationVersionResponseResult `json:"results"`
}

// CurrentMigrationVersionPostBody is the request shape sent to TG to get the migration version
type CurrentMigrationVersionPostBody struct {
	GraphName string `json:"graph_name"`
}

// GetCurrentMigrationNumber returns the current migration number set on the TG instance.
// Returns "" if no migrations have been run
func (c *TigerGraphClient) GetCurrentMigrationNumber(ctx context.Context, graph string) (string, error) {
	response := &CurrentMigrationVersionResponse{}

	postBody := CurrentMigrationVersionPostBody{
		GraphName: graph,
	}

	err := c.Post(ctx, GetCurrentMigrationVersionURL, MetadataGraphName, postBody, response)

	if err != nil {
		return "", err
	}

	if response.Error {
		return "", ErrTigerGraphError
	}

	if len(response.Results[0].LatestMigration) == 0 {
		return "", nil
	}

	latestMigration := response.Results[0].LatestMigration[0]

	mode := latestMigration.Attributes.Mode
	if mode != "up" && mode != "down" { //nolint:goconst
		return "", ErrInvalidMigrationNumber
	}

	if latestMigration.Attributes.Mode == "down" {
		result, err := decrementMigrationNumber(latestMigration.Attributes.MigrationNumber)
		return result, err
	}

	return latestMigration.Attributes.MigrationNumber, nil
}
