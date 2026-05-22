package cli

import (
	"fmt"

	"github.com/spf13/cobra"
)

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate with the broker",
	Args:  cobra.NoArgs,
	RunE: func(cmd *cobra.Command, args []string) error {
		username, _ := cmd.Flags().GetString("username")
		password, _ := cmd.Flags().GetString("password")

		if username == "" {
			fmt.Print("Username: ")
			fmt.Scanln(&username)
		}
		if password == "" {
			fmt.Print("Password: ")
			fmt.Scanln(&password)
		}

		client := apiClient()
		var result struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ExpiresIn    int64  `json:"expires_in"`
			TokenType    string `json:"token_type"`
		}

		if err := client.POST("/auth/login", map[string]string{
			"username": username,
			"password": password,
		}, &result); err != nil {
			return fmt.Errorf("login failed: %w", err)
		}

		cfg := cliCfg
		if cfg == nil {
			cfg = &Config{APIHost: "127.0.0.1", APIPort: 8080}
		}
		cfg.AccessToken = result.AccessToken
		cfg.RefreshToken = result.RefreshToken
		cfg.Username = username

		if err := SaveConfig(cfg); err != nil {
			return fmt.Errorf("save config: %w", err)
		}

		fmt.Println("Login successful")
		return nil
	},
}

func init() {
	loginCmd.Flags().StringP("username", "u", "", "Username")
	loginCmd.Flags().StringP("password", "p", "", "Password")
}
