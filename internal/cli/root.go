package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	cliCfg     *Config
	jsonOutput bool
	flagHost   string
	flagPort   int
)

var rootCmd = &cobra.Command{
	Use:   "octar",
	Short: "OCTAR message broker CLI",
	Long: `Octar is the command-line tool for managing OCTAR message broker.

It connects to the OCTAR management API to configure namespaces,
queues, groups, users, and API keys. It can also start the broker.`,
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if jsonOutput {
			GlobalFormat = FormatJSON
		}

		cfg, err := LoadConfig()
		if err != nil {
			return fmt.Errorf("load config: %w", err)
		}

		if flagHost != "127.0.0.1" {
			cfg.APIHost = flagHost
		}
		if flagPort != 8080 {
			cfg.APIPort = flagPort
		}

		cliCfg = cfg
		return nil
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Help()
	},
}

func Execute() {
	rootCmd.PersistentFlags().StringVarP(&flagHost, "host", "H", "127.0.0.1", "Management API host")
	rootCmd.PersistentFlags().IntVarP(&flagPort, "port", "P", 8080, "Management API port")
	rootCmd.PersistentFlags().BoolVarP(&jsonOutput, "json", "j", false, "Output in JSON format")

	rootCmd.AddCommand(loginCmd)
	rootCmd.AddCommand(namespaceCmd)
	rootCmd.AddCommand(queueCmd)
	rootCmd.AddCommand(groupCmd)
	rootCmd.AddCommand(userCmd)
	rootCmd.AddCommand(apiKeyCmd)
	rootCmd.AddCommand(healthCmd)
	rootCmd.AddCommand(permCmd)
	rootCmd.AddCommand(metricsCmd)
	rootCmd.AddCommand(serverCmd)

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func apiClient() *Client {
	cfg := cliCfg
	if cfg == nil {
		cfg = &Config{APIHost: "127.0.0.1", APIPort: 8080}
	}
	return NewClient(cfg)
}
