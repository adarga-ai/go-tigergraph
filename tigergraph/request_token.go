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
	"encoding/json"
	"net/http"
	"time"
)

// RequestTokenURL is the URL part for getting a token from TigerGraph
const RequestTokenURL = "/requesttoken"

// RequestTokenRequest is the shape of the request to the TigerGraph endpoint for fetching a token
type RequestTokenRequest struct {
	Graph string `json:"graph"`
}

// RequestTokenResponseResults represents the token results shape
type RequestTokenResponseResults struct {
	Token string `json:"token"`
}

// RequestTokenResponse represents the response body from TigerGraph when requesting a token
type RequestTokenResponse struct {
	Code                        string                      `json:"code"`
	ExpirationSecondsSinceEpoch int64                       `json:"expiration"`
	Error                       bool                        `json:"error"`
	Message                     string                      `json:"message"`
	Results                     RequestTokenResponseResults `json:"results"`
}

// Auth authenticates with TigerGraph by hitting the auth endpoint using Basic Auth.
// Will do nothing if a non-expired token for the requested graph already exists in
// the client cache.
func (c *TigerGraphClient) Auth(graph string) error {
	existingToken, exists := c.Tokens[graph]
	if exists && existingToken.Expires.After(time.Now()) {
		return nil
	}

	body := &RequestTokenRequest{Graph: graph}
	tokenResponse := &RequestTokenResponse{}

	data, err := json.Marshal(body)
	if err != nil {
		return err
	}

	request, err := http.NewRequest("POST", c.BaseURL+RequestTokenURL, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.SetBasicAuth(c.BasicAuthUsername, c.BasicAuthPassword)

	err = c.RequestInto(request, tokenResponse)
	if err != nil {
		return err
	}

	c.Tokens[graph] = &Token{
		Value:   tokenResponse.Results.Token,
		Expires: time.Unix(tokenResponse.ExpirationSecondsSinceEpoch, 0),
	}

	return nil
}
