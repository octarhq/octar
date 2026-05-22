package cli

import (
	"github.com/spf13/cobra"
)

var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "Get internal scheduler metrics",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result map[string]any
		if err := client.GET("/internal/metrics", &result); err != nil {
			return err
		}
		printJSON(result)
		return nil
	},
}
