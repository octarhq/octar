package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

type userView struct {
	ID        int     `json:"id"`
	Username  string  `json:"username"`
	Email     *string `json:"email,omitempty"`
	Role      string  `json:"role"`
	CreatedAt string  `json:"created_at"`
	UpdatedAt string  `json:"updated_at,omitempty"`
}

var userCmd = &cobra.Command{
	Use:     "user",
	Aliases: []string{"users", "u"},
	Short:   "Manage users",
}

var userListCmd = &cobra.Command{
	Use:   "list",
	Short: "List users",
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result []userView
		if err := client.GET("/users", &result); err != nil {
			return err
		}
		rows := make([][]string, len(result))
		for i, u := range result {
			email := ""
			if u.Email != nil {
				email = *u.Email
			}
			rows[i] = []string{u.Username, u.Role, email, u.CreatedAt}
		}
		printTable([]string{"USERNAME", "ROLE", "EMAIL", "CREATED"}, rows)
		return nil
	},
}

var userCreateCmd = &cobra.Command{
	Use:   "create <username>",
	Short: "Create a user",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		password, _ := cmd.Flags().GetString("password")
		role, _ := cmd.Flags().GetString("role")
		email, _ := cmd.Flags().GetString("email")

		if password == "" {
			return fmt.Errorf("--password is required")
		}
		if role == "" {
			role = "observer"
		}

		client := apiClient()
		var result userView
		if err := client.POST("/users", map[string]string{
			"username": args[0],
			"password": password,
			"role":     role,
			"email":    email,
		}, &result); err != nil {
			return err
		}
		if GlobalFormat == FormatJSON {
			printJSON(result)
		} else {
			fmt.Printf("User %q created\n", result.Username)
		}
		return nil
	},
}

var userGetCmd = &cobra.Command{
	Use:   "get <username>",
	Short: "Get a user",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result userView
		if err := client.GET("/users/"+args[0], &result); err != nil {
			return err
		}
		printJSON(result)
		return nil
	},
}

var userUpdateCmd = &cobra.Command{
	Use:   "update <username>",
	Short: "Update a user",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		body := make(map[string]any)
		if v, _ := cmd.Flags().GetString("email"); cmd.Flags().Changed("email") {
			body["email"] = v
		}
		if v, _ := cmd.Flags().GetString("role"); cmd.Flags().Changed("role") {
			body["role"] = v
		}
		if v, _ := cmd.Flags().GetString("password"); cmd.Flags().Changed("password") {
			body["password"] = v
		}

		if len(body) == 0 {
			return fmt.Errorf("at least one flag is required: --email, --role, --password")
		}

		client := apiClient()
		var result userView
		if err := client.PATCH("/users/"+args[0], body, &result); err != nil {
			return err
		}
		printJSON(result)
		return nil
	},
}

var userDeleteCmd = &cobra.Command{
	Use:   "delete <username>",
	Short: "Delete a user",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		if err := client.DELETE("/users/"+args[0], nil); err != nil {
			return err
		}
		fmt.Printf("User %q deleted\n", args[0])
		return nil
	},
}

var userPermissionsCmd = &cobra.Command{
	Use:   "permissions <username>",
	Short: "Get user namespace permissions",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		client := apiClient()
		var result struct {
			Username    string              `json:"username"`
			Permissions map[string][]string `json:"permissions"`
		}
		if err := client.GET("/users/"+args[0]+"/permissions", &result); err != nil {
			return err
		}
		if GlobalFormat == FormatJSON {
			printJSON(result)
			return nil
		}
		for ns, perms := range result.Permissions {
			fmt.Printf("  %s: %v\n", ns, perms)
		}
		return nil
	},
}

func init() {
	userCmd.AddCommand(userListCmd)
	userCmd.AddCommand(userCreateCmd)
	userCmd.AddCommand(userGetCmd)
	userCmd.AddCommand(userUpdateCmd)
	userCmd.AddCommand(userDeleteCmd)
	userCmd.AddCommand(userPermissionsCmd)

	userCreateCmd.Flags().StringP("password", "p", "", "Password (required)")
	userCreateCmd.Flags().StringP("role", "r", "observer", "Role: admin, producer, consumer, observer, billing, service")
	userCreateCmd.Flags().StringP("email", "e", "", "Email address")

	userUpdateCmd.Flags().StringP("email", "e", "", "New email")
	userUpdateCmd.Flags().StringP("role", "r", "", "New role")
	userUpdateCmd.Flags().StringP("password", "p", "", "New password")
}
