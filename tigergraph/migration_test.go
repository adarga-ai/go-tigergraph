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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetMigrationsBetweenVersions(t *testing.T) { //nolint:funlen
	cases := []struct {
		name               string
		from               string
		to                 string
		expectedMigrations []string
		expectedMode       string
		expectedError      error
	}{
		{
			name:               "empty from version",
			from:               "",
			to:                 "003",
			expectedMigrations: []string{"000", "001", "002", "003"},
			expectedMode:       "up",
			expectedError:      nil,
		},
		{
			name:               "down migrations",
			from:               "002",
			to:                 "000",
			expectedMigrations: []string{"002", "001"},
			expectedMode:       "down",
			expectedError:      nil,
		},
		{
			name:               "up migrations",
			from:               "000",
			to:                 "002",
			expectedMigrations: []string{"001", "002"},
			expectedMode:       "up",
			expectedError:      nil,
		},
		{
			name:               "from and to are the same",
			from:               "003",
			to:                 "003",
			expectedMigrations: []string{},
			expectedMode:       "up",
			expectedError:      nil,
		},
		{
			name:               "invalid from version",
			from:               "00a",
			to:                 "003",
			expectedMigrations: []string{},
			expectedMode:       "",
			expectedError:      ErrInvalidMigrationNumber,
		},
		{
			name:               "invalid to version",
			from:               "000",
			to:                 "00a",
			expectedMigrations: []string{},
			expectedMode:       "",
			expectedError:      ErrInvalidMigrationNumber,
		},
		{
			name:               "empty to version",
			from:               "000",
			to:                 "",
			expectedMigrations: []string{},
			expectedMode:       "",
			expectedError:      ErrInvalidMigrationNumber,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			migrations, mode, err := getMigrationsBetweenVersions(testCase.from, testCase.to)
			assert.Equal(t, testCase.expectedMigrations, migrations)
			assert.Equal(t, testCase.expectedMode, mode)
			assert.ErrorIs(t, err, testCase.expectedError)
		})
	}
}
