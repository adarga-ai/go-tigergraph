# go-tigergraph

`go-tigergraph` is a Go client for [TigerGraph](https://www.tigergraph.com/). It
provides utilities for authentication, making requests to
["Rest++"](https://docs.tigergraph.com/tigergraph-server/current/api/)
endpoints, and an opinionated migration runner.

This is not an officially supported Adarga product.

## Installation

Add this as a dependency to your project using `go get github.com/Adarga-Ltd/go-tigergraph`.

# Usage

Construct a client using `NewClient` and use its methods:

```go
client := tigergraph.NewClient(
    "http://tigergraph_url:9000",
    "http://tigergraph_url:14240",
    "tgUsername",
    "tgPassword",
)

// Auth is handled for you. 
resp, err := client.GetGraphMetadata("My_Graph")

// Arbitrary GSQL can be executed synchronously. Errors will be detected in the response
// by searching for expected error strings and return codes, and reported in the returned error.
// The returned response is printed to the logger.
myGSQL := `
USE GRAPH My_Graph;

CREATE QUERY ...
`
resp, err := client.RunGSQL(myGSQL)
if err != nil {
    // An error was detected, including syntax errors in the GSQL.
}

// Running installed queries is available via a generic method. Auth tokens are managed
// for you, as is error checking in the response. The response interface must match the returned shape from TigerGraph.
// See get_current_migration_version.go for an example.
err := client.Post("/query/my_installed_query", "My_Graph", requestBodyInterface, &responseInterface)
```

# Migrations

Migrations are `.gsql` files prefixed with a numerical, three digit name, and
suffixed with `.up.gsql` or `.down.gsql`. Up and down migrations are run
automatically based on the specified desired version, and the current version as
tracked in a metadata graph managed by the client.

A directory containing many migrations should be pointed to in the client
constructor.

They can be run with the `client.Migrate()` function like so:

```go
err := client.Migrate(
    "My_Graph",
    "010", // Desired migration version
    "002", // Specify the current migration version if this client has not been used before
    "migrations/v2", // Directory relative to launch, containing migrations
    false, // Migration dry run: will report via logs which migrations would be run but does not run them.
)
if err != nil {
    // handle errors
}
```

Note that migrations are tracked on a per-graph basis, so you must specify which
graph these migrations pertain to.

# Testing

Simply test with `go test ./...`.

# Examples

See the `examples` directory for examples.