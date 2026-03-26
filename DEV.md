## Roadmap

[x] Refactor logging to use zap
    [x] WARN Mode (default)
    [x] INFO -> kafka api calls, high level logic
[x] Fix --to FUTURE support
[x] Add support for --text "my message" frogo put
[x] Add timing stats on ending for frogo get
[x] Test support for SASL PLAIN Clusters


[ ] Adhoc MSK Auth test

## Defects / Tweaks

[x] Add support for configuring profile with FROGO_PROFILE 

[ ] Missing profile: improve error syntax
    [ ] Track time of last_modified for profiles
    [ ] Add suggestion to use recently modified profiles

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
