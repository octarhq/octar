package app

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/octarhq/octar/internal/config"
)

// minimalConfig builds a minimal Config that avoids listening on real ports.
func minimalConfig(dataDir string) *config.Config {
	return &config.Config{
		Server: config.ServerConfig{
			Host:           "127.0.0.1",
			Port:           0,
			MaxConnections: 10,
			ReadTimeout:    5 * time.Second,
			WriteTimeout:   5 * time.Second,
			Inflight:       config.InflightConfig{MaxInflight: 10},
		},
		API:     config.APIConfig{Host: "127.0.0.1", Port: 0},
		Metrics: config.MetricsConfig{Enabled: false},
		PProf:   config.PProfConfig{Enabled: false},
		Storage: config.StorageConfig{
			DataDir: dataDir,
			WAL: config.WALConfig{
				FlushInterval:    25 * time.Millisecond,
				FlushMaxMessages: 100,
				SegmentMaxBytes:  64 << 20,
				Durable:          false,
				SnapshotInterval: 60 * time.Second,
			},
		},
		Auth: config.AuthConfig{
			Enabled: true,
			Providers: config.ProvidersConfig{
				Password: config.PasswordProviderConfig{
					Enabled:    true,
					Priority:   10,
					BcryptCost: 4,
				},
			},
		},
	}
}

// TestAdminCredentialsFile_WrittenOnFirstStartupOnly é o teste que faltava:
// verifica que admin_credentials.txt é escrito apenas na primeira vez e nunca
// sobrescrito em restarts — o bug que existia antes dessa correção.
func TestAdminCredentialsFile_WrittenOnFirstStartupOnly(t *testing.T) {
	dataDir := t.TempDir()
	credPath := filepath.Join(dataDir, "admin_credentials.txt")

	// --- 1ª inicialização: sem senha configurada → deve criar o arquivo ---
	cfg1 := minimalConfig(dataDir)
	// Password vazia = auto-generate
	cfg1.Auth.DefaultAdmin.Username = "admin"
	cfg1.Auth.DefaultAdmin.Password = ""

	app1, err := New(cfg1)
	if err != nil {
		t.Fatalf("New (1ª vez): %v", err)
	}
	app1.DB.Close()

	content1, err := os.ReadFile(credPath)
	if err != nil {
		t.Fatalf("admin_credentials.txt deveria existir após o primeiro boot: %v", err)
	}
	if len(content1) == 0 {
		t.Fatal("admin_credentials.txt está vazio")
	}

	// --- 2ª inicialização: simula restart sem senha → NÃO deve sobrescrever ---
	cfg2 := minimalConfig(dataDir)
	cfg2.Auth.DefaultAdmin.Username = "admin"
	cfg2.Auth.DefaultAdmin.Password = "" // também vazia → gera nova senha aleatória

	app2, err := New(cfg2)
	if err != nil {
		t.Fatalf("New (2ª vez): %v", err)
	}
	defer app2.DB.Close()

	content2, err := os.ReadFile(credPath)
	if err != nil {
		t.Fatalf("admin_credentials.txt deveria continuar existindo: %v", err)
	}

	// O arquivo deve ser IDÊNTICO — se foi sobrescrito o bug voltou
	if string(content1) != string(content2) {
		t.Fatalf("admin_credentials.txt foi sobrescrito no restart!\n1ª vez:\n%s\n2ª vez:\n%s",
			content1, content2)
	}

	// A senha do arquivo deve ser a que realmente funciona no banco
	// (extraindo a senha do arquivo para verificar)
	// Formato: "username: admin\npassword: <pw>\n"
	var username, password string
	for _, line := range splitLines(string(content2)) {
		if len(line) > 10 && line[:10] == "password: " {
			password = line[10:]
		}
		if len(line) > 10 && line[:10] == "username: " {
			username = line[10:]
		}
	}
	if username == "" || password == "" {
		t.Fatalf("não foi possível parsear admin_credentials.txt: %s", content2)
	}
	if !app2.DB.CheckPassword(username, password) {
		t.Fatal("a senha em admin_credentials.txt não bate com o hash no banco após restart — bug de sobrescrita!")
	}
}

func splitLines(s string) []string {
	var lines []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			lines = append(lines, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		lines = append(lines, s[start:])
	}
	return lines
}
