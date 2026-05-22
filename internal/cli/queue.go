package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

type queueSummary struct {
	Name         string `json:"name"`
	Namespace    string `json:"namespace"`
	ActiveGroups int    `json:"active_groups"`
	ConfigCount  int    `json:"config_count"`
}

type queueDetail struct {
	Name         string `json:"name"`
	Namespace    string `json:"namespace"`
	ActiveGroups int    `json:"active_groups"`
	ConfigCount  int    `json:"config_count"`
}

type groupStats struct {
	Key      string `json:"key"`
	Pending  int    `json:"pending"`
	Inflight int    `json:"inflight"`
}

var queueCmd = &cobra.Command{
	Use:     "queue",
	Aliases: []string{"queues", "q"},
	Short:   "Manage queues",
}

var queueListCmd = &cobra.Command{
	Use:   "list",
	Short: "List queues",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result []queueSummary
		if err := client.GET("/queues", &result); err != nil {
			return err
		}
		rows := make([][]string, len(result))
		for i, q := range result {
			rows[i] = []string{q.Name, q.Namespace, fmt.Sprint(q.ActiveGroups), fmt.Sprint(q.ConfigCount)}
		}
		printTable([]string{"NAME", "NAMESPACE", "GROUPS", "CONFIGS"}, rows)
		return nil
	},
}

var queueCreateCmd = &cobra.Command{
	Use:   "create <namespace> <name>",
	Short: "Create a queue",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result queueDetail
		if err := client.POST("/queues", map[string]string{
			"namespace": args[0],
			"name":      args[1],
		}, &result); err != nil {
			return err
		}
		if GlobalFormat == FormatJSON {
			printJSON(result)
		} else {
			fmt.Printf("Queue %q created in namespace %q\n", result.Name, result.Namespace)
		}
		return nil
	},
}

var queueGetCmd = &cobra.Command{
	Use:   "get <namespace> <name>",
	Short: "Get queue details",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result queueDetail
		if err := client.GET("/queues/"+args[0]+"/"+args[1], &result); err != nil {
			return err
		}
		printJSON(result)
		return nil
	},
}

var queueDeleteCmd = &cobra.Command{
	Use:   "delete <namespace> <name>",
	Short: "Delete a queue",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		if err := client.DELETE("/queues/"+args[0]+"/"+args[1], nil); err != nil {
			return err
		}
		fmt.Printf("Queue %q deleted from namespace %q\n", args[1], args[0])
		return nil
	},
}

var queueSnapshotCmd = &cobra.Command{
	Use:   "snapshot <namespace> <name>",
	Short: "Trigger a manual snapshot",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result struct {
			Message string `json:"message"`
		}
		if err := client.POST("/queues/"+args[0]+"/"+args[1]+"/snapshot", nil, &result); err != nil {
			return err
		}
		fmt.Println(result.Message)
		return nil
	},
}

var queueStatsCmd = &cobra.Command{
	Use:   "stats <namespace> <name>",
	Short: "Get queue runtime stats",
	Args:  cobra.ExactArgs(2),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()

		limit, _ := cmd.Flags().GetInt("limit")
		after, _ := cmd.Flags().GetString("after")

		path := "/queues/" + args[0] + "/" + args[1] + "/stats"
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
			Groups     []groupStats `json:"groups"`
			NextCursor string       `json:"next_cursor"`
		}
		if err := client.GET(path, &result); err != nil {
			return err
		}

		if GlobalFormat == FormatJSON {
			printJSON(result)
			return nil
		}

		rows := make([][]string, len(result.Groups))
		for i, g := range result.Groups {
			rows[i] = []string{g.Key, fmt.Sprint(g.Pending), fmt.Sprint(g.Inflight)}
		}
		printTable([]string{"GROUP", "PENDING", "INFLIGHT"}, rows)
		if result.NextCursor != "" {
			fmt.Printf("\nNext cursor: %s\n", result.NextCursor)
		}
		return nil
	},
}

func init() {
	queueCmd.AddCommand(queueListCmd)
	queueCmd.AddCommand(queueCreateCmd)
	queueCmd.AddCommand(queueGetCmd)
	queueCmd.AddCommand(queueDeleteCmd)
	queueCmd.AddCommand(queueSnapshotCmd)
	queueCmd.AddCommand(queueStatsCmd)

	queueStatsCmd.Flags().Int("limit", 100, "Max groups per page")
	queueStatsCmd.Flags().String("after", "", "Cursor for pagination")
}
