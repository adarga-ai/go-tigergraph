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
	"encoding/json"
	"errors"
	"fmt"
)

var (
	// ErrMarshallingJSONL represents failure to turn the supplied argument into JSONL
	ErrMarshallingJSONL = errors.New("failed to marshal into JSONL")

	// ErrLoadingJobRequestFailed represents failure to make a request to TigerGraph
	ErrLoadingJobRequestFailed = errors.New("failed to make request to loading job endpoint")

	// ErrLoadingJobPartialFailure represents a failed request that nevertheless made some successful changes
	ErrLoadingJobPartialFailure = errors.New("not all lines of the JSONL were saved successfully")
)

// LoadingJobObjectResult is the shape of an edge or vertex entry in the
// statistics shape
type LoadingJobObjectResult struct {
	TypeName                   string `json:"typeName"`
	ValidObject                int    `json:"validObject"`
	NoIDFound                  int    `json:"noIdFound"`
	InvalidAttribute           int    `json:"invalidAttribute"`
	InvalidVertexType          int    `json:"invalidVertexType"`
	InvalidPrimaryID           int    `json:"invalidPrimaryId"`
	InvalidSecondaryID         int    `json:"invalidSecondaryId"`
	IncorrectFixedBinaryLength int    `json:"incorrectFixedBinaryLength"`
}

// LoadingJobStatistics is the shape of statistics from the returned
// loading job results
type LoadingJobStatistics struct {
	ValidLine           int                      `json:"validLine"`
	RejectLine          int                      `json:"rejectLine"`
	FailedConditionLine int                      `json:"failedConditionLine"`
	NotEnoughToken      int                      `json:"notEnoughToken"`
	InvalidJSON         int                      `json:"invalidJson"`
	OversizeToken       int                      `json:"oversizeToken"`
	Vertex              []LoadingJobObjectResult `json:"vertex"`
	Edge                []LoadingJobObjectResult `json:"edge"`
}

// LoadingJobResponseResult is the shape of the results value in the response body when saving
// a loading job, edge or vertex
type LoadingJobResponseResult struct {
	SourceFileName string               `json:"sourceFileName"`
	Statistics     LoadingJobStatistics `json:"statistics"`
}

// LoadingJobResponse is the shape of the response body when saving
// a loading job
type LoadingJobResponse struct {
	Version struct {
		Edition string `json:"edition"`
		API     string `json:"api"`
		Schema  int    `json:"schema"`
	} `json:"version"`
	Error   bool                       `json:"error"`
	Message string                     `json:"message"`
	Results []LoadingJobResponseResult `json:"results"`
	Code    string                     `json:"code"`
}

func marshalJSONL(lines []interface{}) ([]byte, error) {
	result := []byte{}
	for i, line := range lines {
		lineBytes, err := json.Marshal(line)
		if err != nil {
			return nil, err
		}

		result = append(result, lineBytes...)
		if i < len(lines)-1 {
			result = append(result, byte('\n'))
		}
	}

	return result, nil
}

// RunLoadingJobJSONL runs a loading job with the given array of interfaces.
func (c *TigerGraphClient) RunLoadingJobJSONL(graphName string, loadingJobName string, lines []interface{}) error {
	bodyBytes, err := marshalJSONL(lines)
	if err != nil {
		return ErrMarshallingJSONL
	}

	queryURL := fmt.Sprintf("/ddl/%s?tag=%s&filename=f", graphName, loadingJobName)

	var response LoadingJobResponse
	err = c.PostRaw(queryURL, graphName, bodyBytes, &response)

	if err != nil {
		return err
	}

	if len(response.Results) != 1 {
		return fmt.Errorf(
			"response does not contain exactly one result. got %d results: %w",
			len(response.Results),
			ErrLoadingJobRequestFailed,
		)
	}

	result := response.Results[0]
	if result.Statistics.ValidLine != len(lines) {
		return fmt.Errorf(
			"tigergraph reported fewer valid JSON lines than were provided. got: %d, expected %d: %w",
			result.Statistics.ValidLine,
			len(lines),
			ErrLoadingJobPartialFailure,
		)
	}

	return nil
}
