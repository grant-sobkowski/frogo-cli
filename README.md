# FROGO

## About the Project

TODO

## Getting Started

### Prerequisites

## Usage

```sh
# Start a mockserver!
frogo mockserver

# In another terminal, create a topic
frogo create-topic hello-world --profile mockserver

# Create a text file with some data
cat <<EOF > ./example.txt
Hello
world
this is
my
topic!
EOF

# Put your data in your topic!
frogo put hello-world --file example.txt --profile mockserver

# Get your topic's data!
frogo get hello-world --from START --to END --profile mockserver

# Get the last 3 messages from your topic!
frogo get hello-world --from index/-3 --to END --profile mockserver
```

## Roadmap

[x] Refactor logging to use zap
    [x] WARN Mode (default)
    [x] INFO -> kafka api calls, high level logic
[x] Fix --to FUTURE support
    [ ] Set default timeout to be never
[ ] Add support for --text "my message" frogo put

[ ] Test support for SASL PLAIN Clusters
[ ] Test support for SCRAM Clusters
[ ] Test support for MSK Clusters

[ ] Add container testing for common configuration scenarios
    [ ] SASL PLAIN
    [ ] SASL SCRAM 256
    [ ] SASL SCRAM 512
    [ ] MSK SASL

## Defects / Tweaks

[ ] Missing profile: improve error syntax
    [ ] Track time of last_modified for profiles
    [ ] Add suggestion to use recently modified profiles

[ ] Organize commands by object
    get, put
    topic -> list, create, delete
    profile -> list, set

[ ] Fix hangup on empty topic read
[ ] Add warning on reading an empty topic

## Acknowledgements


- [franz-go](https://github.com/twmb/franz-go)
- [README Template](https://github.com/othneildrew/Best-README-Template/blob/main/README.md)
- [Conduktor docker compose templates](https://github.com/conduktor/kafka-stack-docker-compose)
