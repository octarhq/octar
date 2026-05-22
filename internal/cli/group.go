package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var groupCmd = &cobra.Command{
	Use:     "group",
	Aliases: []string{"groups", "g"},
	Short:   "Manage group configurations",
}

var groupListCmd = &cobra.Command{
	Use:   "list <namespace> <queue>",
	Short: "List group configurations",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()

		limit, _ := cmd.Flags().GetInt("limit")
		after, _ := cmd.Flags().GetString("after")

		path := "/queues/" + args[0] + "/" + args[1] + "/groups"
		if limit > 0 || after != "" {
			path += "?"
			if limit > 0 {
				path += fmt.Sprintf("limit=%d", limit)
			}
			if after != "" {
				if limit > 0 {
					path += "&"
				}
				path += "after=" + after
			}
		}

		var result struct {
			Configs    []map[string]any `json:"configs"`
			NextCursor string          `json:"next_cursor"`
		}
		if err := client.GET(path, &result); err != nil {
			return err
		}

		if GlobalFormat == FormatJSON {
			printJSON(result)
			return nil
		}

		for _, cfg := range result.Configs {
			key, _ := cfg["key"].(string)
			parallelism, _ := cfg["parallelism"].(float64)
			fmt.Printf("  Key: %s  Parallelism: %.0f\n", key, parallelism)
		}
		if result.NextCursor != "" {
			fmt.Printf("\nNext cursor: %s\n", result.NextCursor)
		}
		return nil
	},
}

var groupGetCmd = &cobra.Command{
	Use:   "get <namespace> <queue> <key>",
	Short: "Get a group configuration",
	Args:  cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result map[string]any
		if err := client.GET("/queues/"+args[0]+"/"+args[1]+"/groups/"+args[2], &result); err != nil {
			return err
		}
		printJSON(result)
		return nil
	},
}

var groupSetCmd = &cobra.Command{
	Use:   "set <namespace> <queue> <key>",
	Short: "Create or update a group configuration",
	Args:  cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()

		body := make(map[string]any)

		if v, _ := cmd.Flags().GetInt("parallelism"); cmd.Flags().Changed("parallelism") {
			body["parallelism"] = v
		}
		if v, _ := cmd.Flags().GetInt("quantum"); cmd.Flags().Changed("quantum") {
			body["quantum"] = v
		}
		if v, _ := cmd.Flags().GetString("lease-timeout"); cmd.Flags().Changed("lease-timeout") {
			body["lease_timeout"] = v
		}
		if v, _ := cmd.Flags().GetInt("retry-max"); cmd.Flags().Changed("retry-max") {
			body["retry"] = map[string]any{"max_attempts": v}
		}
		if v, _ := cmd.Flags().GetString("retry-backoff"); cmd.Flags().Changed("retry-backoff") {
			if body["retry"] == nil {
				body["retry"] = map[string]any{}
			}
			body["retry"].(map[string]any)["backoff"] = v
		}

		var result map[string]any
		if err := client.PUT("/queues/"+args[0]+"/"+args[1]+"/groups/"+args[2], body, &result); err != nil {
			return err
		}
		printJSON(result)
		return nil
	},
}

var groupDeleteCmd = &cobra.Command{
	Use:   "delete <namespace> <queue> <key>",
	Short: "Delete a group configuration",
	Args:  cobra.ExactArgs(3),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		if err := client.DELETE("/queues/"+args[0]+"/"+args[1]+"/groups/"+args[2], nil); err != nil {
			return err
		}
		fmt.Printf("Group config %q deleted\n", args[2])
		return nil
	},
}

func init() {
	groupCmd.AddCommand(groupListCmd)
	groupCmd.AddCommand(groupGetCmd)
	groupCmd.AddCommand(groupSetCmd)
	groupCmd.AddCommand(groupDeleteCmd)

	groupSetCmd.Flags().Int("parallelism", 1, "Parallelism (1=sequential)")
	groupSetCmd.Flags().Int("quantum", 1, "DRR quantum")
	groupSetCmd.Flags().String("lease-timeout", "", "Lease timeout (e.g. 30s, 5m)")
	groupSetCmd.Flags().Int("retry-max", 3, "Max retry attempts")
	groupSetCmd.Flags().String("retry-backoff", "exponential", "Backoff strategy: fixed, linear, exponential")
}
