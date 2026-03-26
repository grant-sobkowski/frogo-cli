//go:build integration

package no_auth

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/grant-sobkowski/frogo-cli/cmd"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v25.2.4"

var (
	container  *redpanda.Container
	brokerAddr string
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	// Testcontainers-go expects a Docker-compatible socket. When running under
	// Podman (rootless), point it at the Podman socket and disable Ryuk, which
	// requires Docker-specific features not available in Podman.
	if os.Getenv("DOCKER_HOST") == "" {
		if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
			sock := xdg + "/podman/podman.sock"
			if _, err := os.Stat(sock); err == nil {
				os.Setenv("DOCKER_HOST", "unix://"+sock)
				os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true")
			}
		}
	}

	var err error
	container, err = redpanda.Run(ctx, redpandaImage)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to start redpanda: %v\n", err)
		os.Exit(1)
	}

	brokerAddr, err = container.KafkaSeedBroker(ctx)
	if err != nil {
		container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to get broker address: %v\n", err)
		os.Exit(1)
	}

	tmpDir, err := os.MkdirTemp("", "frogo-testcontainer-no-auth-*")
	if err != nil {
		container.Terminate(ctx)
		fmt.Fprintf(os.Stderr, "failed to create temp dir: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(filepath.Join(tmpDir, "config.toml"), []byte{}, 0644); err != nil {
		container.Terminate(ctx)
		os.RemoveAll(tmpDir)
		fmt.Fprintf(os.Stderr, "failed to write config: %v\n", err)
		os.Exit(1)
	}

	os.Setenv("FROGO_CONFIG_DIR", tmpDir)

	code := m.Run()

	container.Terminate(ctx)
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

func runCmd(t *testing.T, args ...string) string {
	t.Helper()

	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("failed to create pipe: %v", err)
	}
	os.Stdout = w

	root := cmd.RootCmd()
	root.SetArgs(args)
	root.SetErr(io.Discard)

	execErr := root.Execute()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	r.Close()

	if execErr != nil {
		t.Fatalf("command %v failed: %v\noutput: %s", args, execErr, buf.String())
	}

	return buf.String()
}
