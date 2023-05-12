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
	"testing"
	"time"

	"github.com/adarga-ai/go-tigergraph/tigergraph"
	"github.com/stretchr/testify/assert"
)

const (
	expectedUsername = "username"
	expectedPassword = "password"
)

func TestClientAuth(t *testing.T) {
	getMigrationNumberURL := tigergraph.GetCurrentMigrationVersionURL

	tests := []struct {
		name     string
		username string
		password string
		action   func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer)
	}{
		{
			name:     "successful",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				err := client.Auth(graphName)
				assert.Nil(t, err)
			},
		},
		{
			name:     "wrong password",
			username: expectedUsername,
			password: "wrong",
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				err := client.Auth(graphName)
				assert.Equal(t, tigergraph.ErrNonOK, err)
			},
		},
		{
			name:     "wrong username",
			username: "wrong",
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				err := client.Auth(graphName)
				assert.Equal(t, tigergraph.ErrNonOK, err)
			},
		},
		{
			name:     "multiple auth calls, not expired",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				// Two calls, but another request should not be made since the token hasn't timed out
				// Hence the server was only hit for a token once
				err := client.Auth(graphName)
				assert.Nil(t, err)
				err = client.Auth(graphName)

				assert.Equal(t, 1, len(srv.Calls[tigergraph.RequestTokenURL]))
				assert.Nil(t, err)
			},
		},
		{
			name:     "multiple auth calls, expired",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.Mock(tigergraph.RequestTokenURL, makeDefaultRequestTokenHandler(
					expectedUsername,
					expectedPassword,
					// The token expired 5 minutes ago
					time.Now().Add(-5*time.Minute).Unix(),
				))

				// Two calls. The first call to the token endpoint returned an expired token.
				// So when we call auth again we should make another request.
				err := client.Auth(graphName)
				assert.Nil(t, err)
				err = client.Auth(graphName)

				assert.Equal(t, 2, len(srv.Calls[tigergraph.RequestTokenURL]))
				assert.Nil(t, err)
			},
		},
		{
			name:     "succcessful migration number request",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				srv.MockResponse(getMigrationNumberURL, tigergraph.CurrentMigrationVersionResponse{
					Error: false,
					Results: []tigergraph.CurrentMigrationVersionResponseResult{
						{
							LatestMigration: []tigergraph.MigrationVertex{
								{
									Attributes: tigergraph.MigrationVertexAttributes{
										MigrationNumber: "010",
										Mode:            "up",
									},
								},
							},
						},
					},
				})

				result, err := client.GetCurrentMigrationNumber("MyGraph")
				assert.Nil(t, err)

				assert.Equal(t, "010", result)

				tokenCallCount := len(srv.Calls[tigergraph.RequestTokenURL])
				assert.Equal(t, 1, tokenCallCount)

				migrationVersionCallCount := len(srv.Calls[getMigrationNumberURL])
				assert.Equal(t, 1, migrationVersionCallCount)
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
