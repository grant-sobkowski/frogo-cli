/*
Copyright © 2025 GRANT SOBKOWSKI <grant.sobkowski@gmail.com>
*/

package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/grant-sobkowski/frogo-cli/internal/config"
	"github.com/grant-sobkowski/frogo-cli/internal/mockserver"
	"github.com/spf13/cobra"
)

var mockserverCmd = &cobra.Command{
	Use:   "mockserver",
	Short: "Manage the mock Kafka server",
	Long:  "Run mock instance of kafka cluster",
	RunE:  runMockserver,
}

func init() {
	rootCmd.AddCommand(mockserverCmd)
}

func runMockserver(cmd *cobra.Command, args []string) error {
	// Start the mock server (bootstrapped with test data by default)
	fmt.Println("Starting mock Kafka server...")

	mockServer, err := mockserver.Start(&mockserver.Config{
		NumBrokers: 1,
	})
	if err != nil {
		return err
	}
	defer mockServer.Stop()

	fmt.Println("Mock server bootstrapped successfully")

	mock := config.Profile{
		Name:    "mockserver",
		Brokers: mockServer.Addrs(),
	}

	err = config.WriteProfile(mock)
	if err != nil {
		return err
	}

	fmt.Printf("Mock profile '%v' configured in ~/.frogo/config.toml\n", mock.Name)

	fmt.Printf("Mock server running at: %v\n", mockServer.Addrs())
	fmt.Println("Use 'frogo get -p mockserver ...' or set FROGO_PROFILE=mockserver to connect to this server")
	fmt.Println("Press Ctrl+C to stop the server")

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down mock server...")

	mockServer.Stop()

	return nil
}
