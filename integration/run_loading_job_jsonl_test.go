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
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/adarga-ai/go-tigergraph/tigergraph"
	"github.com/stretchr/testify/assert"
)

type TestPayload struct {
	GUID  string `json:"guid"`
	Value string `json:"value"`
}

func TestLoadingJobJSONL(t *testing.T) { //nolint:funlen
	tests := []struct {
		name     string
		username string
		password string
		action   func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer)
	}{
		{
			name:     "success, single line",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				testLoadingJobURL := fmt.Sprintf(
					"/ddl/%s?tag=%s&filename=f",
					graphName,
					"test_loading_job",
				)

				testPayload := []interface{}{
					TestPayload{
						GUID:  "1234",
						Value: "hello",
					},
				}

				srv.MockResponse(testLoadingJobURL, tigergraph.LoadingJobResponse{
					Results: []tigergraph.LoadingJobResponseResult{
						{
							Statistics: tigergraph.LoadingJobStatistics{
								ValidLine: 1,
							},
						},
					},
				})

				ctx := context.Background()
				err := client.RunLoadingJobJSONL(ctx, graphName, "test_loading_job", testPayload)
				assert.Nil(t, err)

				calls := srv.Calls[testLoadingJobURL]
				assert.Len(t, calls, 1)

				call := calls[0]
				callBytes, err := io.ReadAll(call)
				assert.Nil(t, err)

				expectedCallBody := "{\"guid\":\"1234\",\"value\":\"hello\"}"
				assert.Equal(t, expectedCallBody, string(callBytes))
			},
		},
		{
			name:     "success, multiple lines",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				testLoadingJobURL := fmt.Sprintf(
					"/ddl/%s?tag=%s&filename=f",
					graphName,
					"test_loading_job",
				)

				testPayload := []interface{}{
					TestPayload{
						GUID:  "1234",
						Value: "hello",
					},
					TestPayload{
						GUID:  "222",
						Value: "goodbye",
					},
				}

				srv.MockResponse(testLoadingJobURL, tigergraph.LoadingJobResponse{
					Results: []tigergraph.LoadingJobResponseResult{
						{
							Statistics: tigergraph.LoadingJobStatistics{
								ValidLine: 2,
							},
						},
					},
				})
				ctx := context.Background()
				err := client.RunLoadingJobJSONL(ctx, graphName, "test_loading_job", testPayload)
				assert.Nil(t, err)

				calls := srv.Calls[testLoadingJobURL]
				assert.Len(t, calls, 1)

				call := calls[0]
				callBytes, err := io.ReadAll(call)
				assert.Nil(t, err)

				expectedCallBody := "{\"guid\":\"1234\",\"value\":\"hello\"}\n{\"guid\":\"222\",\"value\":\"goodbye\"}"
				assert.Equal(t, expectedCallBody, string(callBytes))
			},
		},
		{
			name:     "failure, not enough successful lines reported",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				testLoadingJobURL := fmt.Sprintf(
					"/ddl/%s?tag=%s&filename=f",
					graphName,
					"test_loading_job",
				)

				// Sending two lines, but only one valid line in response
				testPayload := []interface{}{
					TestPayload{
						GUID:  "1234",
						Value: "hello",
					},
					TestPayload{
						GUID:  "222",
						Value: "goodbye",
					},
				}

				srv.MockResponse(testLoadingJobURL, tigergraph.LoadingJobResponse{
					Results: []tigergraph.LoadingJobResponseResult{
						{
							Statistics: tigergraph.LoadingJobStatistics{
								ValidLine: 1,
							},
						},
					},
				})

				ctx := context.Background()
				err := client.RunLoadingJobJSONL(ctx, graphName, "test_loading_job", testPayload)
				assert.ErrorIs(t, err, tigergraph.ErrLoadingJobPartialFailure)
			},
		},
		{
			name:     "failure, wrong job name",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				testLoadingJobURL := fmt.Sprintf(
					"/ddl/%s?tag=%s&filename=f",
					graphName,
					"test_loading_job",
				)

				testPayload := []interface{}{
					TestPayload{
						GUID:  "1234",
						Value: "hello",
					},
				}

				srv.MockResponse(testLoadingJobURL, tigergraph.LoadingJobResponse{
					Results: []tigergraph.LoadingJobResponseResult{
						{
							Statistics: tigergraph.LoadingJobStatistics{
								ValidLine: 1,
							},
						},
					},
				})

				ctx := context.Background()
				err := client.RunLoadingJobJSONL(ctx, graphName, "unknown_test_loading_job", testPayload)
				assert.ErrorIs(t, err, tigergraph.ErrNonOK)
			},
		},
		{
			name:     "failure, more than one response object (this should never happen)",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				testLoadingJobURL := fmt.Sprintf(
					"/ddl/%s?tag=%s&filename=f",
					graphName,
					"test_loading_job",
				)

				testPayload := []interface{}{
					TestPayload{
						GUID:  "1234",
						Value: "hello",
					},
				}

				srv.MockResponse(testLoadingJobURL, tigergraph.LoadingJobResponse{
					Results: []tigergraph.LoadingJobResponseResult{
						{
							Statistics: tigergraph.LoadingJobStatistics{
								ValidLine: 1,
							},
						},
						{}, // Another response!
					},
				})

				ctx := context.Background()
				err := client.RunLoadingJobJSONL(ctx, graphName, "test_loading_job", testPayload)
				assert.ErrorIs(t, err, tigergraph.ErrLoadingJobRequestFailed)
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
				test.username,
				test.password,
			)

			test.action(t, client, srv)
		})
	}
}
