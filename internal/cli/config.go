package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"go.yaml.in/yaml/v3"
)

type Config struct {
	APIHost      string `yaml:"api_host"`
	APIPort      int    `yaml:"api_port"`
	AccessToken  string `yaml:"access_token"`
	RefreshToken string `yaml:"refresh_token,omitempty"`
	Username     string `yaml:"username,omitempty"`
}

func configDir() string {
	dir, _ := os.UserHomeDir()
	return filepath.Join(dir, ".octar")
}

func configPath() string {
	return filepath.Join(configDir(), "config.yaml")
}

func ensureDir() error {
	return os.MkdirAll(configDir(), 0755)
}

func LoadConfig() (*Config, error) {
	cfg := &Config{
		APIHost: "127.0.0.1",
		APIPort: 8080,
	}

	if host := os.Getenv("OCTAR_API_HOST"); host != "" {
		cfg.APIHost = host
	}
	if port := os.Getenv("OCTAR_API_PORT"); port != "" {
		fmt.Sscanf(port, "%d", &cfg.APIPort)
	}

	data, err := os.ReadFile(configPath())
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, err
	}

	var fileCfg Config
	if err := yaml.Unmarshal(data, &fileCfg); err != nil {
		return nil, err
	}

	if fileCfg.APIHost != "" {
		cfg.APIHost = fileCfg.APIHost
	}
	if fileCfg.APIPort != 0 {
		cfg.APIPort = fileCfg.APIPort
	}
	if fileCfg.AccessToken != "" {
		cfg.AccessToken = fileCfg.AccessToken
	}
	if fileCfg.RefreshToken != "" {
		cfg.RefreshToken = fileCfg.RefreshToken
	}

	return cfg, nil
}

func SaveConfig(cfg *Config) error {
	if err := ensureDir(); err != nil {
		return err
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	return os.WriteFile(configPath(), data, 0600)
}

func (c *Config) BaseURL() string {
	return fmt.Sprintf("http://%s:%d", c.APIHost, c.APIPort)
}
