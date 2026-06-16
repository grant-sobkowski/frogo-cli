## Roadmap

[x] Refactor logging to use zap
    [x] WARN Mode (default)
    [x] INFO -> kafka api calls, high level logic
[x] Fix --to FUTURE support
[x] Add support for --text "my message" frogo put
[x] Add timing stats on ending for frogo get
[x] Test support for SASL PLAIN Clusters
[x] Add count/<n> support

[x] Add frogo demo command to create example topics
    [x] Determine current structure of integration testing logic -> setupFixtureTopic is just wrapper around frogo topic create, frogo put
    [x] Fix issues with Lsp Config (deprecated mason-lspconfig setup)
    [x] Change runCmd to capture stderr
    [x] Add cobra command definition under cmd/topic_demo.go
    [x] Create wrapper around create topic / put topic (either via commands or function calls)
    [x] Add hello-world topic demo
    
[x] frogo demo
    [x] Add basic scenarios
       [x] basic-json
       [x] 10k-pets-json
    [x] Replace setupFixturetopic logic with frogo demo command
        [x] from-offset-to-offset
        [x] add frogo topic demo-cleanup
    
[ ] add --format 'simple', 'metadata' to frogo get
    [x] Add flag
    [x] Add var to get
    [ ] Decide how to paramaterize OutputRecord function
        [ ] Add to GetState
        [ ] Pass GetState as field to OutputRecord
    [ ] Implement simple, metadata output formats
    
[ ] Build out README.md
  - Introduction
  - Configuring
  - Getting messages
  - Putting messages
  - Running mockserver
  - More info

[ ] Add examples/ folder with python scripts
  [ ] examples/basic/
    [ ] grep
    [ ] jq fields
    [ ] count records by day
    [ ] json-to-csv
  [ ] examples/python/
    [ ] infer-json-schema
    [ ] records-by-day

## Defects / Tweaks

[x] Add support for configuring profile with FROGO_PROFILE 

[x] Missing profile: improve error syntax
    [x] Track time of last_modified for profiles
    [x] Add suggestion to use recently modified profiles

[x] Organize commands by object
    get, put
    topic -> list, create, delete
    profile -> list, set

[x] Fix hangup on empty topic read
    [x] Add warning on reading an empty topic
    [x] Add verbose-mode log for whether stopOnHighWatermarks is set

[x] Remove --wait requirement for --to future
    [x] Set default timeout to be never when streaming

## Development

### Running Tests

```sh
# Unit tests only
go test ./...

# All integration tests
go test ./integration/... -tags integration

# Mockserver integration tests (kfake, no Docker required)
go test ./integration/mockserver/... -tags integration

# Testcontainer integration tests (requires Docker)
go test ./integration/testcontainer/... -tags integration

# Note: For podman users, you'll need to run this first:
systemctl --user start podman.socket


# A specific testcontainer suite
go test ./integration/testcontainer/simple/... -tags integration
```

