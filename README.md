# FROGO

## About the Project

Frogo started out of my frustration for complicated Kafka tooling.

SDKs can be spotty. Existing CLI tools exist, but aren't straight forward to use.

Even to just get started with kafka, you have to worry about:

- Offset management
- Consumer groups (or lack thereof)
- Serialization and Deserialization
- Security configurations

Frogo is an effort to make Kafka development more hackable.

## Getting Started

TODO: Steps to download binary from github

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

# Tail the last 3 messages from your topic!
frogo get hello-world --from index/-3 --to END --profile mockserver
```

## Retrieving Messages

Retrieving messages is done with `frogo get`. 

This command is idempotent, meaning, no consumer groups.

`frogo get --from <from-type> --to <to-type>`

### Supported `--from` Types

| Type | Example Value | Description |
|------|---------------|-------------|
| `START` | | First available offset in the topic |
| `END` | | Current high watermark (latest offset) |
| `offset/<n>` | `offset/0` | Absolute offset (0-based) |
| `index/<n>` | `index/-10` | Relative index from end; negative counts back from latest |
| `unix/<ts>` | `unix/1705312800` | Unix timestamp; ≤10 digits treated as seconds, otherwise milliseconds |
| `iso/<rfc3339>` | `iso/2024-01-15T09:00:00Z` | ISO 8601 / RFC 3339 timestamp |
| `date/<yy:mm:dd>` | `date/24:01:15` | Calendar date; resolves to start of day in `--tz` (default UTC) |

### Supported `--to` Types

| Type | Example Value | Description |
|------|---------------|-------------|
| `END` | | Stop at the current high watermark |
| `FUTURE` | | Stream indefinitely as new messages arrive |
| `offset/<n>` | `offset/100` | Stop at this absolute offset (exclusive) |
| `index/<n>` | `index/-1` | Stop at this relative index from end |
| `unix/<ts>` | `unix/1705312800` | Stop at this unix timestamp |
| `iso/<rfc3339>` | `iso/2024-01-15T17:00:00Z` | Stop at this ISO 8601 / RFC 3339 timestamp |
| `date/<yy:mm:dd>` | `date/24:01:15` | Calendar date; resolves to end of day in `--tz` (default UTC) |
| `count/<n>` | `count/50` | Stop after consuming n messages |


## Writing Messages

Writing messages is done with `frogo put`.

Each line in your input represents one message. I.e. a file with 10 lines will produce 10 messages to your topic.

You can produce using the inline --text option, or via a file using --file.

--format allows you to specify messages with special data. Frogo decodes whatever format you specify before writing to the topic.

```bash
# Produce messages from a file
frogo put my-topic --file messages.txt

# Produce a message from text
frogo put my-topic --text "hello world"
```

### Supported `--format` Options

| Format | Example | Description |
|--------|---------|-------------|
| `utf8` | `hello world` | Plain text (default) |
| `base64` | `aGVsbG8gd29ybGQ=` | Base64-encoded message values. Good option if you need to write binary |
| `record-json` | `{"key": "k1", "value": "hello"}` | Specify kafka message key and value using JSON format |

## Using mockserver

`frogo mockserver` starts a local in-memory Kafka broker for testing without a real cluster.

When started, it automatically writes connection configurations to profile: `mockserver`

```bash
# In one terminal, start the mock server
frogo mockserver

# In another terminal, use it with the auto-configured profile
export FROGO_PROFILE="mockserver"
frogo put my-topic --text "hello world"
frogo get my-topic --from START --to END
```

## Configuration

Frogo stores connection settings in `~/.frogo/config.toml` as named profiles. Most commands require a profile to be configured before use.

### Profiles

A profile is a named set of connection settings. You can select a profile three ways, in order of precedence:

1. **Flag:** `-p <name>` or `--profile <name>`
2. **Environment variable:** `FROGO_PROFILE=<name>`
3. **Default:** falls back to the `default` profile

```bash
frogo get my-topic --from START --to END --profile prod
# or
export FROGO_PROFILE=prod
frogo get my-topic --from START --to END
```

### Setting Configurations

Configuration can be set directly in `~/.frogo/config.toml` or via `frogo config set`:

```bash
# Set brokers for the default profile
frogo config set brokers localhost:9092

# Set brokers for a named profile
frogo config set brokers broker1:9092,broker2:9092 --profile prod

# Configure SASL authentication
frogo config set security-protocol sasl_ssl --profile prod
frogo config set sasl-mechanism SCRAM-SHA-256 --profile prod
frogo config set sasl-username myuser --profile prod
frogo config set sasl-password mypassword --profile prod
```

### Available Configuration Fields

| Field | Example | Description |
|-------|---------|-------------|
| `brokers` | `localhost:9092` | Comma-delimited list of broker addresses |
| `security_protocol` | `sasl_ssl` | Connection security: `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl` |
| `sasl_mechanism` | `SCRAM-SHA-256` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `AWS_MSK_IAM` |
| `sasl_username` | `myuser` | SASL username |
| `sasl_password` | `mypassword` | SASL password |
| `message_max_bytes` | `10485760` | Maximum producer message size in bytes |
| `receive_message_max_bytes` | `10485760` | Maximum receive message size in bytes |
| `tls_ca_cert_file` | `/path/to/ca.pem` | Path to a custom TLS CA certificate |
| `tls_client_cert_file` | `/path/to/client.crt` | Path to the TLS client certificate (mTLS) |
| `tls_client_key_file` | `/path/to/client.key` | Path to the TLS client key (mTLS) |
| `tls_skip_verify` | `true` | Skip TLS certificate verification (not recommended for production) |


## Acknowledgements

- [franz-go](https://github.com/twmb/franz-go)
- [Redpanda](https://github.com/redpanda-data/redpanda)
- [README Template](https://github.com/othneildrew/Best-README-Template/blob/main/README.md)
- [Conduktor docker compose templates](https://github.com/conduktor/kafka-stack-docker-compose)
