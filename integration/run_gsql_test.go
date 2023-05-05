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
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/Adarga-Ltd/go-tigergraph/tigergraph"
	"github.com/stretchr/testify/assert"
)

func TestClientRunGSQL(t *testing.T) {
	tests := []struct {
		name     string
		username string
		password string
		action   func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer)
	}{
		{
			name:     "success, query escapes string",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				gsqlBody := "CREATE GRAPH Relationships()"

				responseString := fmt.Sprintf("Installing query...\n\n%s\n", tigergraph.SuccessString)
				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(responseString))
					if err != nil {
						t.Errorf("failed to write to response")
					}
				})

				err := client.RunGSQL(gsqlBody)
				assert.Nil(t, err)

				calls := srv.Calls[tigergraph.FileURL]
				assert.Len(t, calls, 1)

				call := calls[0]
				expectedCallBody := bytes.NewBufferString(url.QueryEscape(gsqlBody))
				assert.Equal(t, expectedCallBody, call)
			},
		},
		{
			name:     "no response code",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				gsqlBody := "CREATE GRAPH Relationships()"

				responseString := "Graph already exists."
				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(responseString))
					if err != nil {
						t.Errorf("failed to write to response")
					}
				})

				err := client.RunGSQL(gsqlBody)
				assert.ErrorIs(t, err, tigergraph.ErrGSQLFailure)

				calls := srv.Calls[tigergraph.FileURL]
				assert.Len(t, calls, 1)
			},
		},
		{
			name:     "non zero response code",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				gsqlBody := "CREATE GRAPH Relationships()"

				responseString := "Failed to install queries.\n__GSQL__RETURN__CODE__,211\n"
				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(responseString))
					if err != nil {
						t.Errorf("failed to write to response")
					}
				})

				err := client.RunGSQL(gsqlBody)
				assert.ErrorIs(t, err, tigergraph.ErrGSQLFailure)

				calls := srv.Calls[tigergraph.FileURL]
				assert.Len(t, calls, 1)
			},
		},
		{
			name:     "non OK http response code",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				gsqlBody := "CREATE GRAPH Relationships()"

				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusBadRequest)
				})

				err := client.RunGSQL(gsqlBody)
				assert.ErrorIs(t, err, tigergraph.ErrNonOK)

				calls := srv.Calls[tigergraph.FileURL]
				assert.Len(t, calls, 1)
			},
		},
		{
			name:     "semantic check failure",
			username: expectedUsername,
			password: expectedPassword,
			action: func(t *testing.T, client *tigergraph.TigerGraphClient, srv *MockTigerGraphServer) {
				gsqlBody := "CREATE GRAPH Relationships()"

				responseString := "Semantic Check Fails: These queries could not be found anywhere: [non_existent].\n__GSQL__RETURN__CODE__,0\n"
				srv.Mock(tigergraph.FileURL, func(w http.ResponseWriter, r *http.Request) {
					_, err := w.Write([]byte(responseString))
					if err != nil {
						t.Errorf("failed to write to response")
					}
				})

				err := client.RunGSQL(gsqlBody)
				assert.ErrorIs(t, err, tigergraph.ErrGSQLFailure)

				calls := srv.Calls[tigergraph.FileURL]
				assert.Len(t, calls, 1)
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
