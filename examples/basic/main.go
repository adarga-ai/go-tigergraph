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
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/adarga-ai/go-tigergraph/tigergraph"
)

type TestVertex struct {
	ID string  `json:"id"`
	X  float32 `json:"x"`
	Y  float32 `json:"y"`
}

type InCircleResponseResult struct {
	Vertices []tigergraph.ResponseVertex[TestVertex] `json:"vertices"`
}

func main() {
	tgURL := os.Getenv("TG_URL")
	tgFileURL := os.Getenv("TG_FILE_URL")
	tgUsername := os.Getenv("TG_USERNAME")
	tgPassword := os.Getenv("TG_PASSWORD")

	fmt.Println(tgURL, tgFileURL, tgUsername, tgPassword)

	ctx := context.Background()
	client := tigergraph.NewClient(tgURL, tgFileURL, tgUsername, tgPassword)

	// Run migration
	err := client.Migrate(ctx, "TestGraph", "000", "", "./migrations", false)
	if err != nil {
		fmt.Println("failed to migrate DB: ", err)
		return
	}

	// Put items in DB
	verticesInterface := make([]interface{}, 0)
	vertices := []TestVertex{
		{
			ID: "0",
			X:  0.1,
			Y:  0.1,
		},
		{
			ID: "1",
			X:  10,
			Y:  0,
		},
		{
			ID: "2",
			X:  -0.3,
			Y:  0.5,
		},
	}
	for _, v := range vertices {
		verticesInterface = append(verticesInterface, v)
	}
	err = client.RunLoadingJobJSONL(ctx, "TestGraph", "test_vertex_loading_job", verticesInterface)
	if err != nil {
		fmt.Println("failed to run loading job: ", err)
		return
	}

	// Get vertices that are inside a circle at position 0, 0 and radius 5
	fmt.Println("\nlooking for vertices inside circle at (0, 0) and radius 5")
	var result tigergraph.TigerGraphResponse[InCircleResponseResult]
	err = client.Get(ctx, "/query/vertices_in_circle?cx=0.0&cy=0.0&r=5", "TestGraph", &result)
	if err != nil {
		fmt.Println("failed to get vertices in circle: ", err)
		return
	}

	fmt.Printf("got %d results:\n", len(result.Results[0].Vertices))
	for _, v := range result.Results[0].Vertices {
		fmt.Printf("%s: (%f, %f)\n", v.Attributes.ID, v.Attributes.X, v.Attributes.Y)
	}

	// Get vertices that are inside a circle at position 11, 0 and radius 3
	fmt.Println("\nlooking for vertices inside circle at (11, 0) and radius 3")
	var otherResult tigergraph.TigerGraphResponse[InCircleResponseResult]
	err = client.Get(ctx, "/query/vertices_in_circle?cx=11.0&cy=0.0&r=3", "TestGraph", &otherResult)
	if err != nil {
		fmt.Println("failed to get vertices in circle: ", err)
		return
	}

	fmt.Printf("got %d results:\n", len(otherResult.Results[0].Vertices))
	for _, v := range otherResult.Results[0].Vertices {
		fmt.Printf("%s: (%f, %f)\n", v.Attributes.ID, v.Attributes.X, v.Attributes.Y)
	}
}
