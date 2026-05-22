package cli

import (
	"github.com/spf13/cobra"
)

var permCmd = &cobra.Command{
	Use:     "permissions",
	Aliases: []string{"perms"},
	Short:   "List available permissions",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result struct {
			Global    []map[string]string `json:"global"`
			Namespace []map[string]string `json:"namespace"`
		}
		if err := client.GET("/permissions", &result); err != nil {
			return err
		}
		printJSON(result)
		return nil
	},
}
