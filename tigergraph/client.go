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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var (
	// ErrPost represents a failure to make a POST request
	ErrPost = errors.New("failed to make POST request to TigerGraph")

	// ErrGet represents a failure to make a GET request
	ErrGet = errors.New("failed to make GET request to TigerGraph")

	// ErrNonOK represents a non-OK status code (200) was returned
	ErrNonOK = errors.New("TigerGraph returned non-OK status code")

	// ErrBodyReadFailed  represents a failure to read the TigerGraph response body
	ErrBodyReadFailed = errors.New("failed to read response body")

	// ErrTigerGraphError means that an error is present on the returned response body
	ErrTigerGraphError = errors.New("error in the response body")

	// ErrRequestFailed represents a failure to make a request to TigerGraph
	ErrRequestFailed = errors.New("failed request")

	// ErrNotOneResult represents a response shape that does not contain exactly one result
	ErrNotOneResult = errors.New("TigerGraph did not respond with exactly one result")
)

const (
	// PingURL is the URL to make a ping request
	PingURL = "/api/ping"

	// TigerGraphDateTimeFormat is the date format used by TigerGraph
	TigerGraphDateTimeFormat = "2006-01-02 15:04:05"
)

// Token is used to track active TigerGraph tokens on the client
type Token struct {
	Value   string
	Expires time.Time
}

// TigerGraphClient provides an idiomatic interface to TigerGraph
type TigerGraphClient struct {
	BaseURL           string
	BaseFileURL       string
	BasicAuthUsername string
	BasicAuthPassword string
	Tokens            map[string]*Token
}

// NewClient creates a new TigerGraphClient
func NewClient(
	baseURL string,
	baseFileURL string,
	username string,
	password string,
) *TigerGraphClient {
	return &TigerGraphClient{
		BaseURL:           baseURL,
		BaseFileURL:       baseFileURL,
		Tokens:            make(map[string]*Token),
		BasicAuthUsername: username,
		BasicAuthPassword: password,
	}
}

// Get makes a GET request to the TigerGraph endpoint. This handles auth automatically.
func (c *TigerGraphClient) Get(ctx context.Context, queryURL string, graph string, result interface{}) error {
	request, err := http.NewRequestWithContext(ctx, "GET", c.BaseURL+queryURL, nil)
	if err != nil {
		return err
	}

	if err = c.ApplyTokenAuth(request, graph); err != nil {
		return err
	}

	return c.RequestInto(request, result)
}

// Post makes a POST request to the TigerGraph endpoint. This handles auth automatically.
func (c *TigerGraphClient) Post(ctx context.Context, queryURL string, graph string, body interface{}, result interface{}) error {
	requestBody, err := json.Marshal(body)
	if err != nil {
		return err
	}

	return c.PostRaw(ctx, queryURL, graph, requestBody, result)
}

// PostRaw makes a POST request to the TigerGraph endpoint with some given bytes. This handles auth automatically.
func (c *TigerGraphClient) PostRaw(ctx context.Context, queryURL string, graph string, body []byte, result interface{}) error {
	request, err := http.NewRequestWithContext(ctx, "POST", c.BaseURL+queryURL, bytes.NewBuffer(body))
	if err != nil {
		return err
	}

	err = c.ApplyTokenAuth(request, graph)
	if err != nil {
		return err
	}

	return c.RequestInto(request, result)
}

// RequestInto takes an HTTP request, performs it and unmarshals the response into the supplied
// result argument.
func (c *TigerGraphClient) RequestInto(req *http.Request, result interface{}) error {
	resp, err := http.DefaultClient.Do(req)

	if err != nil {
		return err
	}

	defer func() {
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return ErrNonOK
	}

	jsonBytes, err := io.ReadAll(resp.Body)

	if err != nil {
		return err
	}

	err = json.Unmarshal(jsonBytes, result)

	if err != nil {
		return fmt.Errorf("failed to unmarshal response. response: %s, %w", string(jsonBytes), err)
	}

	return nil
}

// CreateGSQLServerRequest returns a Request instance that is authenticated and ready to
// pass to RequestInto. This is useful if headers need to be changed by the caller (such as setting the Content-Type).
func (c *TigerGraphClient) CreateGSQLServerRequest(ctx context.Context, method string, url string, body string) (*http.Request, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		method,
		c.BaseFileURL+url,
		strings.NewReader(body),
	)
	if err != nil {
		return nil, err
	}

	c.ApplyBasicAuth(request)

	return request, nil
}

// ApplyTokenAuth takes a request and authenticates it for a specified graph, using
// TigerGraph's RESTPP token authentication endpoint.
//
// https://docs.tigergraph.com/tigergraph-server/current/api/built-in-endpoints#_request_a_token
func (c *TigerGraphClient) ApplyTokenAuth(req *http.Request, graph string) error {
	err := c.Auth(req.Context(), graph)
	if err != nil {
		return err
	}

	authToken := fmt.Sprintf("Bearer %s", c.Tokens[graph].Value)
	req.Header.Add("Authorization", authToken)
	return nil
}

// ApplyBasicAuth takes a request and authenticates it generally as a TigerGraph user for GSQL server requests
//
// https://docs.tigergraph.com/tigergraph-server/current/api/authentication#_gsql_server_requests
func (c *TigerGraphClient) ApplyBasicAuth(req *http.Request) {
	req.SetBasicAuth(c.BasicAuthUsername, c.BasicAuthPassword)
}
