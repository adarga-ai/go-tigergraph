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
package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"

	"github.com/Adarga-Ltd/go-tigergraph/tigergraph"
	"github.com/stretchr/testify/assert"
)

func TestIsInitialised(t *testing.T) {
	tests := []struct {
		name   string
		action func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer)
	}{
		{
			name: "returns error when unable to reach tigergraph for metadata",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.Mock(tigergraph.GetGraphMetadataQueryURL, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})

				result, err := client.CheckIsInitialised()
				assert.ErrorIs(t, err, tigergraph.ErrNonOK)
				assert.False(t, result)
			},
		},
		{
			name: "returns false and no error, when metadata response contains appropriate message",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   true,
					Message: "Graph name ClientMetadata cannot be found. For whatever reason.",
				})

				result, err := client.CheckIsInitialised()
				assert.Nil(t, err)
				assert.False(t, result)
			},
		},
		{
			name: "returns false and an error, when metadata response contains unexpected message",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   true,
					Message: "You are not authenticated",
				})

				result, err := client.CheckIsInitialised()
				assert.ErrorIs(t, err, tigergraph.ErrUnknownInitialisationCheckFailure)
				assert.False(t, result)
			},
		},
		{
			name: "returns false and an error, when metadata response contains no error but has no graph name in response",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   false,
					Message: "",
					Results: &tigergraph.GraphMetadataResponseResult{},
				})

				result, err := client.CheckIsInitialised()
				assert.ErrorIs(t, err, tigergraph.ErrUnknownInitialisationCheckFailure)
				assert.False(t, result)
			},
		},
		{
			name: "returns true and no error, when metadata response contains no error and has correct graph name in response",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   false,
					Message: "",
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				result, err := client.CheckIsInitialised()
				assert.Nil(t, err)
				assert.True(t, result)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := NewMockServer(expectedUsername, expectedPassword)
			defer srv.Close()

			client := tigergraph.NewClient(
				srv.HTTPServer.URL,
				srv.HTTPServer.URL,
				expectedUsername,
				expectedPassword,
			)

			test.action(t, client, srv)
		})
	}
}

func TestMigrate(t *testing.T) {
	exampleGraphName := "MyGraph"
	migrationDir := "../testutils/migrations/v1"

	successResponseString := fmt.Sprintf("Installing query...\n\n%s\n", tigergraph.SuccessString)
	migrationUpsertURL := tigergraph.UpsertURL + "/" + tigergraph.MetadataGraphName

	assertUpsertPayload := func(t *testing.T, b []byte, migrationNumber string, mode string) {
		t.Helper()
		var asStruct tigergraph.MigrationUpsertPayload
		err := json.Unmarshal(b, &asStruct)
		assert.Nil(t, err)
		assert.Len(t, asStruct.Vertices.Migration, 1)

		for _, v := range asStruct.Vertices.Migration {
			assert.Equal(t, v.MigrationNumber.Value, migrationNumber)
			assert.Equal(t, v.Mode.Value, mode)
		}
	}

	makeLatestMigrationVertexResponse := func(version string, mode string) tigergraph.CurrentMigrationVersionResponse {
		return tigergraph.CurrentMigrationVersionResponse{
			Results: []tigergraph.CurrentMigrationVersionResponseResult{
				{
					LatestMigration: []tigergraph.MigrationVertex{
						{
							Attributes: tigergraph.MigrationVertexAttributes{
								MigrationNumber: version,
								Mode:            mode,
								GraphName:       exampleGraphName,
							},
						},
					},
				},
			},
		}
	}

	emptyLatestMigrationVertexResponse := tigergraph.CurrentMigrationVersionResponse{
		Results: []tigergraph.CurrentMigrationVersionResponseResult{
			{
				LatestMigration: []tigergraph.MigrationVertex{},
			},
		},
	}

	oneAcceptedUpsertVertexResponse := tigergraph.UpsertResponse{
		Results: []tigergraph.UpsertResponseResult{
			{
				AcceptedVertices: 1,
			},
		},
	}

	tests := []struct {
		name   string
		action func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer)
	}{
		{
			name: "does not run the initialisation gsql if already initialised",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				// Already initialised, successful response
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				// We are on migration 001 already
				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, makeLatestMigrationVertexResponse("001", "up"))

				err := client.Migrate(
					exampleGraphName,
					"001",
					"",
					migrationDir,
					false,
				)
				assert.Nil(t, err)

				// Nothing to do, neither migrations nor upserts of versions
				assert.Zero(t, len(srv.Calls[tigergraph.FileURL]))
				assert.Zero(t, len(srv.Calls[migrationUpsertURL]))
			},
		},
		{
			name: "runs the initialisation gsql and then first migration if not initialised",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				// Not initialised
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   true,
					Message: tigergraph.ExpectedFailurePrefix,
				})

				// No migrations have been run
				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, emptyLatestMigrationVertexResponse)

				// Upsert is successful
				srv.MockResponse(migrationUpsertURL, oneAcceptedUpsertVertexResponse)

				// Migration is successful
				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				// There are two calls to run GSQL
				err := client.Migrate(
					exampleGraphName,
					"000",
					"",
					migrationDir,
					false,
				)
				assert.Nil(t, err)
				assert.Equal(t, 2, len(srv.Calls[tigergraph.FileURL]))

				// First to initialise the new graph
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, url.QueryEscape(tigergraph.InitFileString), string(firstCallBytes))

				// Second to run the 000 migration
				secondCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][1])
				assert.Nil(t, err)
				assert.Equal(t, "example+000+up", string(secondCallBytes))

				assert.Equal(t, 1, len(srv.Calls[migrationUpsertURL]))
				firstUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][0])
				assert.Nil(t, err)
				assertUpsertPayload(t, firstUpsertCallBytes, "000", "up")
			},
		},
		{
			name: "inserts migration vertices for init but does not run migrations",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   true,
					Message: tigergraph.ExpectedFailurePrefix,
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, makeLatestMigrationVertexResponse("001", "up"))

				srv.MockResponse(migrationUpsertURL, oneAcceptedUpsertVertexResponse)

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"001",
					migrationDir,
					false,
				)
				assert.Nil(t, err)

				// There is one call to the file URL containing the init file
				assert.Equal(t, 1, len(srv.Calls[tigergraph.FileURL]))
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, url.QueryEscape(tigergraph.InitFileString), string(firstCallBytes))

				assert.Equal(t, 2, len(srv.Calls[migrationUpsertURL]))
				firstUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][0])
				assert.Nil(t, err)
				assertUpsertPayload(t, firstUpsertCallBytes, "000", "up")

				secondUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][1])
				assert.Nil(t, err)
				assertUpsertPayload(t, secondUpsertCallBytes, "001", "up")
			},
		},
		{
			name: "some migrations are init, others are actually run",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   true,
					Message: tigergraph.ExpectedFailurePrefix,
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, makeLatestMigrationVertexResponse("000", "up"))

				srv.MockResponse(migrationUpsertURL, oneAcceptedUpsertVertexResponse)

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"000",
					migrationDir,
					false,
				)
				assert.Nil(t, err)

				// There are two calls to the file URL
				assert.Equal(t, 2, len(srv.Calls[tigergraph.FileURL]))
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, url.QueryEscape(tigergraph.InitFileString), string(firstCallBytes))

				// Migration 001 is actually run
				secondCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][1])
				assert.Nil(t, err)
				assert.Equal(t, "example+001+up", string(secondCallBytes))

				// But two upserts are run (000 as part of init)
				assert.Equal(t, 2, len(srv.Calls[migrationUpsertURL]))

				firstUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][0])
				assert.Nil(t, err)
				assertUpsertPayload(t, firstUpsertCallBytes, "000", "up")

				secondUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][1])
				assert.Nil(t, err)
				assertUpsertPayload(t, secondUpsertCallBytes, "001", "up")
			},
		},
		{
			name: "metadata is initialised and init version is still set",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, makeLatestMigrationVertexResponse("001", "up"))

				err := client.Migrate(
					exampleGraphName,
					"001",
					"000",
					migrationDir,
					false,
				)
				assert.Nil(t, err)

				// There are no calls to the file URL
				assert.Equal(t, 0, len(srv.Calls[tigergraph.FileURL]))

				// No upserts are run
				assert.Equal(t, 0, len(srv.Calls[migrationUpsertURL]))
			},
		},
		{
			name: "runs down migrations",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, makeLatestMigrationVertexResponse("001", "up"))

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				srv.MockResponse(migrationUpsertURL, oneAcceptedUpsertVertexResponse)

				err := client.Migrate(
					exampleGraphName,
					"000",
					"",
					migrationDir,
					false,
				)
				assert.Nil(t, err)

				// There is one call to the file URL (the down migration)
				assert.Equal(t, 1, len(srv.Calls[tigergraph.FileURL]))
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, "example+001+down", string(firstCallBytes))

				// One upsert is run (001 down)
				assert.Equal(t, 1, len(srv.Calls[migrationUpsertURL]))

				firstUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][0])
				assert.Nil(t, err)
				assertUpsertPayload(t, firstUpsertCallBytes, "001", "down")
			},
		},
		{
			name: "init check fails",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   true,
					Message: "something unexpected",
				})

				srv.MockResponse(migrationUpsertURL, oneAcceptedUpsertVertexResponse)

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"001",
					migrationDir,
					false,
				)

				assert.Equal(t, tigergraph.ErrUnknownInitialisationCheckFailure, err)
				assert.Equal(t, 0, len(srv.Calls[tigergraph.FileURL]))
				assert.Equal(t, 0, len(srv.Calls[migrationUpsertURL]))
				assert.Equal(t, 0, len(srv.Calls[tigergraph.GetCurrentMigrationVersionURL]))
			},
		},
		{
			name: "failure when running migration stops immediately",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, emptyLatestMigrationVertexResponse)

				srv.MockResponse(migrationUpsertURL, oneAcceptedUpsertVertexResponse)

				// FileURL fails
				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"",
					migrationDir,
					false,
				)
				assert.ErrorIs(t, err, tigergraph.ErrTigerGraphSchemaSetUpFailed)

				// There is only one call to the file URL (the failing 000 migration)
				// because we don't try to proceed with any more
				assert.Equal(t, 1, len(srv.Calls[tigergraph.FileURL]))
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, "example+000+up", string(firstCallBytes))

				// No upserts are run, because no migration succeeded
				assert.Equal(t, 0, len(srv.Calls[migrationUpsertURL]))
			},
		},
		{
			name: "failing to commit the migration version exits immediately when not initialised",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Error:   true,
					Message: tigergraph.ExpectedFailurePrefix,
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, emptyLatestMigrationVertexResponse)

				srv.Mock(migrationUpsertURL, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"000",
					migrationDir,
					false,
				)
				assert.ErrorIs(t, err, tigergraph.ErrNonOK)

				// Just the initialisation was done, no actual migrations
				assert.Equal(t, 1, len(srv.Calls[tigergraph.FileURL]))
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, url.QueryEscape(tigergraph.InitFileString), string(firstCallBytes))

				// This is the request that fails
				assert.Equal(t, 1, len(srv.Calls[migrationUpsertURL]))
				firstUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][0])
				assert.Nil(t, err)
				assertUpsertPayload(t, firstUpsertCallBytes, "000", "up")
			},
		},
		{
			name: "failing to commit the migration version exits immediately when initialised",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, emptyLatestMigrationVertexResponse)

				srv.Mock(migrationUpsertURL, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"000",
					migrationDir,
					false,
				)
				assert.ErrorIs(t, err, tigergraph.ErrNonOK)

				// One migration is made successfully
				assert.Equal(t, 1, len(srv.Calls[tigergraph.FileURL]))
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, "example+000+up", string(firstCallBytes))

				// Only one upsert is made (but fails)
				assert.Equal(t, 1, len(srv.Calls[migrationUpsertURL]))
				firstUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][0])
				assert.Nil(t, err)
				assertUpsertPayload(t, firstUpsertCallBytes, "000", "up")
			},
		},
		{
			name: "no migrations are run if the check to get current migration fails",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				// Fail to get current migration version
				srv.Mock(tigergraph.GetCurrentMigrationVersionURL, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusInternalServerError)
				})

				srv.MockResponse(migrationUpsertURL, oneAcceptedUpsertVertexResponse)

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"000",
					migrationDir,
					false,
				)
				assert.ErrorIs(t, err, tigergraph.ErrNonOK)

				// There are no calls to the file URL (no migration is made, no init to be done)
				assert.Equal(t, 0, len(srv.Calls[tigergraph.FileURL]))

				// No upserts of the migration version is made
				assert.Equal(t, 0, len(srv.Calls[migrationUpsertURL]))
			},
		},
		{
			name: "last migration run was a down migration",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, makeLatestMigrationVertexResponse("001", "down"))

				srv.MockResponse(migrationUpsertURL, oneAcceptedUpsertVertexResponse)

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"000",
					migrationDir,
					false,
				)
				assert.Nil(t, err)

				// There is one call to the file URL
				assert.Equal(t, 1, len(srv.Calls[tigergraph.FileURL]))

				// Migration 001 is run, because it was down last
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, "example+001+up", string(firstCallBytes))

				// One upsert is run (001 as part of init)
				assert.Equal(t, 1, len(srv.Calls[migrationUpsertURL]))

				firstUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][0])
				assert.Nil(t, err)
				assertUpsertPayload(t, firstUpsertCallBytes, "001", "up")
			},
		},
		{
			name: "migration dry run",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				// No migrations have been run
				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, emptyLatestMigrationVertexResponse)

				// There are no calls to run GSQL
				err := client.Migrate(
					exampleGraphName,
					"000",
					"",
					migrationDir,
					true,
				)
				assert.Nil(t, err)
				assert.Equal(t, 0, len(srv.Calls[tigergraph.FileURL]))

				// Upsert also does not take place
				assert.Equal(t, 0, len(srv.Calls[migrationUpsertURL]))
			},
		},
		{
			// Should not be possible, but this is a valuable thing to catch and log in case someone
			// accidentally sets an invalid migration mode manually in TG
			name: "graph contains invalid migration mode",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				// Note "don" instead of "down"
				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, makeLatestMigrationVertexResponse("001", "don"))

				err := client.Migrate(
					exampleGraphName,
					"000",
					"",
					migrationDir,
					false,
				)
				assert.ErrorIs(t, err, tigergraph.ErrInvalidMigrationNumber)

				// There are no calls to file endpoint (no migrations or init)
				assert.Equal(t, 0, len(srv.Calls[tigergraph.FileURL]))

				// No upsert is run
				assert.Equal(t, 0, len(srv.Calls[migrationUpsertURL]))
			},
		},
		{
			name: "upsert returns non zero inserted vertices",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(tigergraph.GetGraphMetadataQueryURL+"?graph=ClientMetadata", tigergraph.GraphMetadataResponse{
					Results: &tigergraph.GraphMetadataResponseResult{
						GraphName: tigergraph.MetadataGraphName,
					},
				})

				srv.MockResponse(tigergraph.GetCurrentMigrationVersionURL, makeLatestMigrationVertexResponse("001", "down"))

				srv.MockResponse(migrationUpsertURL, tigergraph.UpsertResponse{
					Results: []tigergraph.UpsertResponseResult{
						{
							AcceptedVertices: 0,
						},
					},
				})

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(successResponseString))
					if err != nil {
						t.Errorf("failed to write to response writer: %s\n", err)
					}
				})

				err := client.Migrate(
					exampleGraphName,
					"001",
					"000",
					migrationDir,
					false,
				)
				assert.ErrorIs(t, err, tigergraph.ErrTigerGraphSchemaSetUpFailed)

				// There is one call to the file URL
				assert.Equal(t, 1, len(srv.Calls[tigergraph.FileURL]))

				// Migration 001 is run, because it was down last
				firstCallBytes, err := io.ReadAll(srv.Calls[tigergraph.FileURL][0])
				assert.Nil(t, err)
				assert.Equal(t, "example+001+up", string(firstCallBytes))

				// One upsert is run, but this returns a value for accepted vertices which is not 1
				assert.Equal(t, 1, len(srv.Calls[migrationUpsertURL]))

				firstUpsertCallBytes, err := io.ReadAll(srv.Calls[migrationUpsertURL][0])
				assert.Nil(t, err)
				assertUpsertPayload(t, firstUpsertCallBytes, "001", "up")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := NewMockServer(expectedUsername, expectedPassword)
			defer srv.Close()

			client := tigergraph.NewClient(
				srv.HTTPServer.URL,
				srv.HTTPServer.URL,
				expectedUsername,
				expectedPassword,
			)

			test.action(t, client, srv)
		})
	}
}
