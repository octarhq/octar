package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var healthCmd = &cobra.Command{
	Use:   "health",
	Short: "Check broker health",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result struct {
			Status  string `json:"status"`
			Version string `json:"version"`
			Checks  []struct {
				Name   string `json:"name"`
				Status string `json:"status"`
				Detail string `json:"detail,omitempty"`
			} `json:"checks"`
		}
		if err := client.GET("/health", &result); err != nil {
			return fmt.Errorf("health check failed: %w", err)
		}

		if GlobalFormat == FormatJSON {
			printJSON(result)
			return nil
		}

		fmt.Printf("Status:  %s\n", result.Status)
		fmt.Printf("Version: %s\n", result.Version)
		fmt.Println("Checks:")
		for _, c := range result.Checks {
			status := "✓"
			if c.Status != "ok" {
				status = "✗"
			}
			fmt.Printf("  %s %s", status, c.Name)
			if c.Detail != "" {
				fmt.Printf(" (%s)", c.Detail)
			}
			fmt.Println()
		}
		return nil
	},
}
