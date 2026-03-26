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
frogo topic create hello-world --profile mockserver

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

## Acknowledgements

- [franz-go](https://github.com/twmb/franz-go)
- [Redpanda](https://github.com/redpanda-data/redpanda)
- [README Template](https://github.com/othneildrew/Best-README-Template/blob/main/README.md)
- [Conduktor docker compose templates](https://github.com/conduktor/kafka-stack-docker-compose)
