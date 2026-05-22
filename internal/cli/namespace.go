package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

type namespaceView struct {
	ID        int            `json:"id"`
	Name      string         `json:"name"`
	Config    map[string]any `json:"config,omitempty"`
	CreatedAt string         `json:"created_at"`
	UpdatedAt string         `json:"updated_at"`
}

var namespaceCmd = &cobra.Command{
	Use:     "namespace",
	Aliases: []string{"ns", "namespaces"},
	Short:   "Manage namespaces",
}

var namespaceListCmd = &cobra.Command{
	Use:   "list",
	Short: "List namespaces",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result []namespaceView
		if err := client.GET("/namespaces", &result); err != nil {
			return err
		}
		rows := make([][]string, len(result))
		for i, ns := range result {
			rows[i] = []string{ns.Name, ns.CreatedAt, ns.UpdatedAt}
		}
		printTable([]string{"NAME", "CREATED", "UPDATED"}, rows)
		return nil
	},
}

var namespaceCreateCmd = &cobra.Command{
	Use:   "create <name>",
	Short: "Create a namespace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result namespaceView
		if err := client.POST("/namespaces", map[string]string{"name": args[0]}, &result); err != nil {
			return err
		}
		if GlobalFormat == FormatJSON {
			printJSON(result)
		} else {
			fmt.Printf("Namespace %q created\n", result.Name)
		}
		return nil
	},
}

var namespaceGetCmd = &cobra.Command{
	Use:   "get <name>",
	Short: "Get namespace details",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result namespaceView
		if err := client.GET("/namespaces/"+args[0], &result); err != nil {
			return err
		}
		printJSON(result)
		return nil
	},
}

var namespaceDeleteCmd = &cobra.Command{
	Use:   "delete <name>",
	Short: "Delete a namespace",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		if err := client.DELETE("/namespaces/"+args[0], nil); err != nil {
			return err
		}
		fmt.Printf("Namespace %q deleted\n", args[0])
		return nil
	},
}

func init() {
	namespaceCmd.AddCommand(namespaceListCmd)
	namespaceCmd.AddCommand(namespaceCreateCmd)
	namespaceCmd.AddCommand(namespaceGetCmd)
	namespaceCmd.AddCommand(namespaceDeleteCmd)
}
