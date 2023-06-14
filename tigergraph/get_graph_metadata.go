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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// GetGraphMetadataQueryURL is the TigerGraph URL to get schema metadata
const GetGraphMetadataQueryURL = "/gsqlserver/gsql/schema"

// GraphMetadataAttributeType is the type attribute on a vertex type attribute
type GraphMetadataAttributeType struct {
	Name string `json:"Name"`
}

// GraphMetadataAttribute is the attribute on a vertex type
type GraphMetadataAttribute struct {
	AttributeName string                     `json:"AttributeName"`
	AttributeType GraphMetadataAttributeType `json:"AttributeType"`
}

// GraphMetadataVertexTypePrimaryID is the primary ID attribute in a vertex type
type GraphMetadataVertexTypePrimaryID struct {
	AttributeType        GraphMetadataAttributeType `json:"AttributeType"`
	PrimaryIDAsAttribute bool                       `json:"PrimaryIdAsAttribute"`
	AttributeName        string                     `json:"AttributeName"`
}

// GraphMetadataVertexType is a vertex type in the metadata response body
type GraphMetadataVertexType struct {
	Config     map[string]string                `json:"Config"`
	IsLocal    bool                             `json:"IsLocal"`
	Attributes []GraphMetadataAttribute         `json:"Attributes"`
	PrimaryID  GraphMetadataVertexTypePrimaryID `json:"PrimaryId"`
	Name       string                           `json:"Name"`
}

// GraphMetadataEdgePair is an edge pair
type GraphMetadataEdgePair struct {
	From string `json:"From"`
	To   string `json:"To"`
}

// GraphMetadataEdgeType is an edge type in the metadata response body
type GraphMetadataEdgeType struct {
	IsDirected         bool                     `json:"IsDirected"`
	ToVertexTypeName   string                   `json:"ToVertexTypeName"`
	Config             map[string]string        `json:"Config"`
	IsLocal            bool                     `json:"IsLocal"`
	Attributes         []GraphMetadataAttribute `json:"Attributes"`
	FromVertexTypeName string                   `json:"FromVertexTypeName"`
	EdgePairs          []GraphMetadataEdgePair  `json:"EdgePairs"`
	Name               string                   `json:"Name"`
}

// GraphMetadataResponseResult is the result shape contained in the response when getting the graph metadata
type GraphMetadataResponseResult struct {
	GraphName   string                    `json:"GraphName"`
	VertexTypes []GraphMetadataVertexType `json:"VertexTypes"`
	EdgeTypes   []GraphMetadataEdgeType   `json:"EdgeTypes"`
}

// GraphMetadataResponse is the whole TigerGraph response for a metadata query
type GraphMetadataResponse struct {
	Message string                       `json:"message"`
	Error   bool                         `json:"error"`
	Results *GraphMetadataResponseResult `json:"results"`
}

// GraphMetadataPartialResponse does not specify a type for the "results" key, because we do not yet
// know the type when we first get the response (until we check the error status).
type GraphMetadataPartialResponse struct {
	Message string          `json:"message"`
	Error   bool            `json:"error"`
	Results json.RawMessage `json:"results"`
}

// GetGraphMetadata returns the graph metadata for a given graph name
func (c *TigerGraphClient) GetGraphMetadata(ctx context.Context, graphName string) (*GraphMetadataResponse, error) {
	urlString := fmt.Sprintf("%s?graph=%s", GetGraphMetadataQueryURL, graphName)
	req, err := c.CreateGSQLServerRequest(ctx, http.MethodGet, urlString, "")
	if err != nil {
		return nil, err
	}

	resp := &GraphMetadataPartialResponse{}
	err = c.RequestInto(req, resp)
	if err != nil {
		return nil, err
	}

	// Note that error attribute isn't checked here because the message and
	// error are of special interest to callers of this method

	// TigerGraph comes back with an empty string in the error case, for the "results" attribute.
	// We have to "try" to unmarshal and just return nothing if the unmarshal fails
	var responseResult GraphMetadataResponseResult
	err = json.Unmarshal(resp.Results, &responseResult)
	if err != nil {
		return &GraphMetadataResponse{
			Message: resp.Message,
			Error:   resp.Error,
		}, nil
	}

	return &GraphMetadataResponse{
		Message: resp.Message,
		Error:   resp.Error,
		Results: &responseResult,
	}, nil
}
