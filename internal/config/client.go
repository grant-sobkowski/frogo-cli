package config

import (
	"fmt"

	"github.com/grant-sobkowski/frogo-cli/internal/logger"
	"github.com/twmb/franz-go/pkg/kgo"
)

func Client(profile string) (*kgo.Client, error) {
	opts, err := ReadProfile(profile)
	if err != nil {
		return nil, fmt.Errorf("failed to read profile for client config: %w", err)
	}

	opts = append(opts, kgo.WithHooks(&logger.KafkaHook{}))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("error while instantiating client: %w", err)
	}

	return cl, nil
}
