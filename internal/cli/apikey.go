package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var apiKeyCmd = &cobra.Command{
	Use:     "api-key",
	Aliases: []string{"apikey", "api-keys", "keys"},
	Short:   "Manage API keys",
}

var apiKeyCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create an API key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		namespace, _ := cmd.Flags().GetString("namespace")
		permissions, _ := cmd.Flags().GetStringSlice("permission")

		if namespace == "" {
			return fmt.Errorf("--namespace is required")
		}
		if len(permissions) == 0 {
			permissions = []string{"publish", "consume"}
		}

		client := apiClient()
		var result struct {
			Key         string   `json:"key"`
			SubjectID   string   `json:"subject_id"`
			Namespace   string   `json:"namespace"`
			Permissions []string `json:"permissions"`
		}
		if err := client.POST("/auth/api-keys", map[string]any{
			"name":        args[0],
			"namespace":   namespace,
			"permissions": permissions,
		}, &result); err != nil {
			return err
		}
		if GlobalFormat == FormatJSON {
			printJSON(result)
		} else {
			fmt.Printf("API Key: %s\n", result.Key)
			fmt.Printf("Namespace: %s\n", result.Namespace)
			fmt.Printf("Permissions: %v\n", result.Permissions)
			fmt.Println("Save this key — it will not be shown again")
		}
		return nil
	},
}

var apiKeyListCmd = &cobra.Command{
	Use:   "list",
	Short: "List API keys",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result []struct {
			SubjectID   string   `json:"subject_id"`
			Namespace   string   `json:"namespace"`
			Permissions []string `json:"permissions"`
			Prefix      string   `json:"prefix,omitempty"`
		}
		if err := client.GET("/auth/api-keys", &result); err != nil {
			return err
		}
		rows := make([][]string, len(result))
		for i, k := range result {
			rows[i] = []string{k.SubjectID, k.Namespace, fmt.Sprint(k.Permissions), k.Prefix}
		}
		printTable([]string{"NAME", "NAMESPACE", "PERMISSIONS", "PREFIX"}, rows)
		return nil
	},
}

var apiKeyRevokeCmd = &cobra.Command{
	Use:   "revoke <key>",
	Short: "Revoke an API key",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result struct {
			Success bool `json:"success"`
		}
		if err := client.DELETEWithBody("/auth/api-keys", map[string]string{"key": args[0]}, &result); err != nil {
			return err
		}
		fmt.Println("API key revoked")
		return nil
	},
}

func init() {
	apiKeyCmd.AddCommand(apiKeyCreateCmd)
	apiKeyCmd.AddCommand(apiKeyListCmd)
	apiKeyCmd.AddCommand(apiKeyRevokeCmd)

	apiKeyCreateCmd.Flags().StringP("namespace", "n", "", "Namespace (required)")
	apiKeyCreateCmd.Flags().StringSliceP("permission", "p", nil, "Permissions (repeatable)")
}
