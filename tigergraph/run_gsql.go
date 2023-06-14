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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const (
	// FileURL is the tiger graph URL to run arbitrary GSQL
	FileURL = "/gsqlserver/gsql/file"

	// SuccessString is the string we expect to find in the response that indicates successful return
	SuccessString = "__GSQL__RETURN__CODE__,0"

	// SemanticFailureString is the string to look for which indicates that some error in semantics
	// occurred when running the GSQL
	SemanticFailureString = "Semantic Check Fails:"
)

var (
	// ErrGSQLFailure is an error in the case of being unable to run GSQL on the TG server
	ErrGSQLFailure = errors.New("failed to execute GSQL")
)

// RunGSQL executes arbitrary GSQL on a remote TG instance using the client.
// If any failure is detected, an error is returned.  Note however that this
// does not mean that none of the GSQL was executed. You may need to inspect the
// logged response to identify what succeeded in the request.
func (c *TigerGraphClient) RunGSQL(ctx context.Context, body string) error {
	escapedBody := url.QueryEscape(body)

	request, err := c.CreateGSQLServerRequest(ctx, http.MethodPost, FileURL, escapedBody)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/octet-stream")

	resp, err := http.DefaultClient.Do(request)

	if err != nil {
		return ErrRequestFailed
	}

	defer func() {
		resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf(
			"http request came back with non 200 status code. code: %d: %w",
			resp.StatusCode,
			ErrNonOK,
		)
	}

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	respString := string(respBytes)
	respLines := strings.Split(respString, "\n")
	if len(respLines) < 2 { //nolint:gomnd
		return fmt.Errorf(
			"not enough returned lines in GSQL response. full response: %s: %w",
			respString,
			ErrGSQLFailure,
		)
	}

	if strings.Contains(respString, SemanticFailureString) {
		return fmt.Errorf(
			"a semantic failure was found in the response. full response: %s: %w",
			respString,
			ErrGSQLFailure,
		)
	}

	responseCodeLine := respLines[len(respLines)-2]
	if responseCodeLine != SuccessString {
		return fmt.Errorf(
			"GSQL response did not contain expected success code. response code was: %s\nfull data was: %s\n: %w",
			responseCodeLine,
			respString,
			ErrGSQLFailure,
		)
	}

	return nil
}
