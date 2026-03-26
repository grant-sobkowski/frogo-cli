//go:build integration

package sasl_ssl

import (
	"strings"
	"testing"
)

func TestContainers_SASLSSLTopicLifecycle(t *testing.T) {
	topic := "tc-sasl-ssl-topic"

	t.Run("ConfigureProfile", func(t *testing.T) {
		runCmd(t, "config", "set", "brokers", brokerAddr, "--profile", "test")
		runCmd(t, "config", "set", "security-protocol", "sasl_ssl", "--profile", "test")
		runCmd(t, "config", "set", "sasl-mechanism", "SCRAM-SHA-256", "--profile", "test")
		runCmd(t, "config", "set", "sasl-username", "test-user", "--profile", "test")
		runCmd(t, "config", "set", "sasl-password", "test-user-password", "--profile", "test")
		runCmd(t, "config", "set", "tls-ca-cert-file", caCertPath, "--profile", "test")
	})

	t.Run("CreateTopic", func(t *testing.T) {
		out := runCmd(t, "topic", "create", topic, "--profile", "test")
		if !strings.Contains(out, topic) {
			t.Errorf("expected topic name in output, got: %s", out)
		}
	})

	t.Run("ListTopics", func(t *testing.T) {
		out := runCmd(t, "topic", "list", "--profile", "test")
		if !strings.Contains(out, topic) {
			t.Errorf("expected %q in topic list, got: %s", topic, out)
		}
	})

	t.Run("ProduceMessages", func(t *testing.T) {
		runCmd(t, "put", topic, "--text", "hello\nworld\nfrom sasl_ssl", "--profile", "test")
	})

	t.Run("ConsumeMessages", func(t *testing.T) {
		out := runCmd(t, "get", topic, "--from", "START", "--to", "END", "--profile", "test")
		for _, msg := range []string{"hello", "world", "from sasl_ssl"} {
			if !strings.Contains(out, msg) {
				t.Errorf("expected %q in consumed output, got: %s", msg, out)
			}
		}
	})

	t.Run("DeleteTopic", func(t *testing.T) {
		out := runCmd(t, "topic", "delete", topic, "--profile", "test")
		if !strings.Contains(out, topic) {
			t.Errorf("expected topic name in output, got: %s", out)
		}
	})

	t.Run("ListTopicsAfterDelete", func(t *testing.T) {
		out := runCmd(t, "topic", "list", "--profile", "test")
		if strings.Contains(out, topic) {
			t.Errorf("expected %q to be absent from topic list after delete, got: %s", topic, out)
		}
	})
}
