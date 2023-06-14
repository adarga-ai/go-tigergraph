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
	_ "embed"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// MetadataGraphName is the name of the graph the client stores metadata in
	MetadataGraphName = "ClientMetadata"

	// ExpectedFailurePrefix is the start of the error received when the client has not initialised the metadata
	ExpectedFailurePrefix = "Graph name " + MetadataGraphName + " cannot be found."

	trackMigrationFailureTemplate = "failed to commit migration version to metadata graph.\n" +
		"IMPORTANT: this requires manual intervention. This migration record failed to be set,\n" +
		"but the migration was run successfully. The easiest resolution to this is to set the init version\n" +
		"to be the migration version printed in this log, as that will ensure the new migration vertex is added\n" +
		"and will skip the version.\nI.e. set TIGER_GRAPH_MIGRATION_INIT_VERSION=%s as an env var. Original error: %w"
)

var (
	// ErrUnknownInitialisationCheckFailure occurs when an error is returned, but it is not the
	// error which signals that initialisation hasn't happened yet
	ErrUnknownInitialisationCheckFailure = errors.New("initialisation check failed for an unknown reason")

	// ErrTigerGraphSchemaSetUpFailed means that a failure occurred when running migrations
	ErrTigerGraphSchemaSetUpFailed = errors.New("failed to set up schema in tiger graph")

	// ErrInvalidMigrationNumber means that a supplied migration number was invalid
	ErrInvalidMigrationNumber = errors.New("migration number was invalid")
)

// CheckIsInitialised determines if the metadata graph has been initialised
// and ready for use.
func (c *TigerGraphClient) CheckIsInitialised(ctx context.Context) (bool, error) {
	meta, err := c.GetGraphMetadata(ctx, MetadataGraphName)
	if err != nil {
		return false, err
	}

	if !meta.Error && meta.Results.GraphName == MetadataGraphName {
		return true, nil
	}

	if strings.HasPrefix(meta.Message, ExpectedFailurePrefix) {
		return false, nil
	}

	return false, ErrUnknownInitialisationCheckFailure
}

// InitFileString is the content of the initialisation GSQL as a string
//
//go:embed gsql/metadata_init.gsql
var InitFileString string

// Migrate checks the status of migrations in the metadata graph and uses that
// information along with the specified version to determine which migrations to
// run.
//
// If the metadata graph does not yet exist, it is created and initialised.
func (c *TigerGraphClient) Migrate(
	ctx context.Context,
	graph string,
	version string,
	initVersion string,
	migrationFileDir string,
	dryRun bool,
) error {
	isInitialised, err := c.CheckIsInitialised(ctx)
	if err != nil {
		return err
	}

	if !isInitialised {
		if err = c.RunGSQL(ctx, InitFileString); err != nil {
			return err
		}

		initialVersion := initVersion
		if initialVersion != "" {
			migrationNumbers, migrationMode, err := getMigrationsBetweenVersions("", initialVersion)

			if err != nil {
				return fmt.Errorf(
					"failed to determine the initial migrations to run: initialVersion: %s, %w",
					initialVersion,
					err,
				)
			}

			for _, migrationNumber := range migrationNumbers {
				if err = c.commitMigrationVersion(ctx, graph, migrationNumber, migrationMode); err != nil {
					return fmt.Errorf("failed to commit migration number: migrationNumber: %s, %w", migrationNumber, err)
				}
			}
		}
	}

	currentMigrationNumber, err := c.GetCurrentMigrationNumber(ctx, graph)
	if err != nil {
		return fmt.Errorf("failed to get current migration number from TigerGraph: %w", err)
	}

	desiredMigrationNumber := version
	migrationNumbers, migrationMode, err := getMigrationsBetweenVersions(currentMigrationNumber, desiredMigrationNumber)
	if err != nil {
		return err
	}

	for _, migrationNumber := range migrationNumbers {
		if dryRun {
			continue
		}
		if err = c.tryMigrateStep(ctx, migrationNumber, migrationMode, migrationFileDir); err != nil {
			return err
		}
		if err = c.commitMigrationVersion(ctx, graph, migrationNumber, migrationMode); err != nil {
			return fmt.Errorf(trackMigrationFailureTemplate, migrationNumber, err)
		}
	}
	return nil
}

func decrementMigrationNumber(n string) (string, error) {
	asInt, err := strconv.ParseInt(n, 10, 32)
	if err != nil {
		return "", ErrInvalidMigrationNumber
	}

	return fmt.Sprintf("%03d", asInt-1), nil
}

func getMigrationsBetweenVersions(from string, to string) ([]string, string, error) {
	result := make([]string, 0)

	if to == "" {
		return result, "", ErrInvalidMigrationNumber
	}

	if from == "" {
		from = "-001"
	}

	fromInt, err := strconv.ParseInt(from, 10, 32)
	if err != nil {
		return result, "", ErrInvalidMigrationNumber
	}

	toInt, err := strconv.ParseInt(to, 10, 32)
	if err != nil {
		return result, "", ErrInvalidMigrationNumber
	}

	minInt := fromInt
	if toInt < minInt {
		minInt = toInt
	}

	maxInt := toInt
	if fromInt > maxInt {
		maxInt = fromInt
	}

	for i := minInt + 1; i <= maxInt; i++ {
		versionString := fmt.Sprintf("%03d", i)
		result = append(result, versionString)
	}

	mode := "up"
	if fromInt > toInt {
		mode = "down"
		sort.Slice(result, func(i, j int) bool {
			return result[i] > result[j]
		})
	}

	return result, mode, nil
}

func (c *TigerGraphClient) tryMigrateStep(ctx context.Context, number string, mode string, migrationFileDir string) error {
	files, err := os.ReadDir(migrationFileDir)
	if err != nil {
		return err
	}

	expectedSuffix := fmt.Sprintf("%s.gsql", mode)

	for _, file := range files {
		if strings.HasPrefix(file.Name(), number+"_") && strings.HasSuffix(file.Name(), expectedSuffix) {
			fileName := migrationFileDir + "/" + file.Name()
			err = c.migrateFile(ctx, fileName)
			if err != nil {
				return fmt.Errorf("failed to set up TG schema: %s, %w", err, ErrTigerGraphSchemaSetUpFailed)
			}

			return nil
		}
	}

	return fmt.Errorf(
		"failed to run migration, no file with migration number found. number: %s, mode: %s",
		number,
		mode,
	)
}

func (c *TigerGraphClient) migrateFile(ctx context.Context, fileName string) error {
	bytes, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}

	err = c.RunGSQL(ctx, string(bytes))
	if err != nil {
		return err
	}

	return nil
}

// MigrationVertexPayloadValue is an object containing a "value" attribute
type MigrationVertexPayloadValue[T any] struct {
	Value T `json:"value"`
}

// MigrationVertexPayload is the shape of a migration vertex being stored in the vertex upsert payload
type MigrationVertexPayload struct {
	GraphName       MigrationVertexPayloadValue[string]    `json:"graph_name"`
	MigrationNumber MigrationVertexPayloadValue[string]    `json:"migration_number"`
	Mode            MigrationVertexPayloadValue[string]    `json:"mode"`
	CreatedAt       MigrationVertexPayloadValue[time.Time] `json:"created_at"`
}

// MigrationVerticesPayload is the map to all vertices in the payload
type MigrationVerticesPayload struct {
	Migration map[string]MigrationVertexPayload `json:"Migration"`
}

// MigrationUpsertPayload is the whole payload sent to the upsert vertices endpoint for migrations
type MigrationUpsertPayload struct {
	Vertices MigrationVerticesPayload `json:"vertices"`
}

func (c *TigerGraphClient) commitMigrationVersion(ctx context.Context, graph string, version string, mode string) error {
	createdAt := time.Now()
	id := fmt.Sprintf("%s_%s_%s", version, mode, createdAt.Format(time.RFC3339))
	payload := MigrationUpsertPayload{
		MigrationVerticesPayload{
			map[string]MigrationVertexPayload{
				id: {
					GraphName:       MigrationVertexPayloadValue[string]{graph},
					MigrationNumber: MigrationVertexPayloadValue[string]{version},
					Mode:            MigrationVertexPayloadValue[string]{mode},
					CreatedAt:       MigrationVertexPayloadValue[time.Time]{createdAt},
				},
			},
		},
	}

	res, err := c.Upsert(ctx, MetadataGraphName, payload)
	if err != nil {
		return err
	}

	if res.AcceptedVertices != 1 {
		return fmt.Errorf(
			"upsert of migration vertex returned an unexpected number of accepted vertices. accepted: %d but expected only 1. error type: %w",
			res.AcceptedVertices,
			ErrTigerGraphSchemaSetUpFailed,
		)
	}

	return nil
}
