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
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/adarga-ai/go-tigergraph/tigergraph"
)

type handlerFunc = func(w http.ResponseWriter, r *http.Request)

// MockTigerGraphServer behaves like a real TigerGraph HTTP server.
// It keeps track of calls made to each endpoint.
type MockTigerGraphServer struct {
	HTTPServer *httptest.Server
	Calls      map[string][]io.Reader

	Username     string
	Password     string
	mockHandlers map[string]handlerFunc
}

// NewMockServer creates a new *MockTigerGraphServer ready to receive requests.
// Close() must be called on the result, e.g. via a defer call immediately after creation.
func NewMockServer(username, password string) *MockTigerGraphServer {
	result := &MockTigerGraphServer{
		Username:   username,
		Password:   password,
		HTTPServer: nil,
		Calls:      make(map[string][]io.Reader),
	}

	result.setInitialMocks()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The request body has to be copied because reading it closes the ReadCloser
		bodyBytes, _ := io.ReadAll(r.Body)
		result.Calls[r.URL.String()] = append(result.Calls[r.URL.String()], bytes.NewBuffer(bodyBytes))
		r.Body = io.NopCloser(bytes.NewBuffer(bodyBytes))

		handler, found := result.mockHandlers[r.URL.String()]
		if !found {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		handler(w, r)
	}))

	result.HTTPServer = srv
	return result
}

func (ms *MockTigerGraphServer) setInitialMocks() {
	ms.mockHandlers = map[string]handlerFunc{
		tigergraph.RequestTokenURL: makeDefaultRequestTokenHandler(
			ms.Username,
			ms.Password,
			// Create a token that lasts 5 minutes by default
			time.Now().Add(5*time.Minute).Unix(), //nolint:gomnd
		),
		tigergraph.PingURL: func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		},
	}
}

// Close closes the mock server.
func (ms *MockTigerGraphServer) Close() {
	ms.HTTPServer.Close()
}

// Reset resets all mocks and calls on the mock server.
// This is useful if you want to avoid recreating a mock server every test.
func (ms *MockTigerGraphServer) Reset() {
	ms.Calls = make(map[string][]io.Reader)
	ms.setInitialMocks()
}

// Mock allows an arbitrary handler to be set for a given URL.
// This is useful for e.g. returning a different response code
func (ms *MockTigerGraphServer) Mock(url string, f handlerFunc) {
	ms.mockHandlers[url] = f
}

// MockResponse sets the mock server to respond with a given response on the supplied url.
func (ms *MockTigerGraphServer) MockResponse(url string, response interface{}) {
	ms.Mock(url, func(w http.ResponseWriter, r *http.Request) {
		responseBytes, err := json.Marshal(response)
		if err != nil {
			// This shouldn't happen, just panic
			panic("Failed to unmarshall token response from mock server.")
		}

		_, err = w.Write(responseBytes)
		if err != nil {
			panic("Failed to write response.")
		}
	})
}

func makeDefaultRequestTokenHandler(username, password string, expiration int64) handlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		suppliedUsername, suppliedPassword, ok := r.BasicAuth()
		if suppliedUsername != username || suppliedPassword != password || !ok {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		tokenResponse := &tigergraph.RequestTokenResponse{
			ExpirationSecondsSinceEpoch: expiration,
			Error:                       false,
			Results: tigergraph.RequestTokenResponseResults{
				Token: "sometoken",
			},
		}

		tokenResponseBytes, err := json.Marshal(tokenResponse)
		if err != nil {
			// This shouldn't happen, just panic
			panic("Failed to unmarshall token response from mock server.")
		}

		_, err = w.Write(tokenResponseBytes)
		if err != nil {
			panic("Failed to write response.")
		}
	}
}
