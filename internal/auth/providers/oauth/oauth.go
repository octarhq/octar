package oauth

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/83codes/octar/internal/auth/authenticator"
	"github.com/83codes/octar/internal/auth/identity"
	"github.com/83codes/octar/internal/config"
)

type OAuthProvider struct {
	config config.OAuthConfig
	client *http.Client
}

type OAuthConfig struct {
	Provider     string // google, github, microsoft, oidc
	ClientID     string
	ClientSecret string
	RedirectURL  string
	Scopes       []string
	TeamsMap     map[string]string // team name -> namespace mapping
}

type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token,omitempty"`
	IDToken      string `json:"id_token,omitempty"`
}

type UserInfo struct {
	ID      string   `json:"id"`
	Email   string   `json:"email"`
	Name    string   `json:"name"`
	Picture string   `json:"picture,omitempty"`
	Login   string   `json:"login,omitempty"` // GitHub
	Teams   []string `json:"teams,omitempty"`
}

func NewOAuthProvider(cfg config.OAuthConfig) *OAuthProvider {
	return &OAuthProvider{
		config: cfg,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (o *OAuthProvider) Name() string {
	return "oauth:" + o.config.Provider
}

func (o *OAuthProvider) Priority() int {
	return 50
}

func (o *OAuthProvider) Authenticate(ctx context.Context, req authenticator.AuthRequest) (*identity.Identity, string, error) {
	if req.Token == "" {
		return nil, "", nil
	}

	// For OAuth, the token would be an ID token or access token
	// Validate and extract user info based on provider
	var userInfo *UserInfo
	var err error

	switch o.config.Provider {
	case "google":
		userInfo, err = o.validateGoogleToken(ctx, req.Token)
	case "github":
		userInfo, err = o.validateGitHubToken(ctx, req.Token)
	case "microsoft":
		userInfo, err = o.validateMicrosoftToken(ctx, req.Token)
	default:
		return nil, "", fmt.Errorf("unsupported OAuth provider: %s", o.config.Provider)
	}

	if err != nil {
		return nil, "", err
	}

	ns := o.mapTeamToNamespace(userInfo)

	id := &identity.Identity{
		SubjectID:   userInfo.ID,
		SubjectType: identity.SubjectUser,
		AccountID:   userInfo.Email,
		Namespace:   ns,
		Roles:       []string{"user"},
		AuthMethod:  identity.AuthMethodOAuth,
		Namespaces:  map[string][]string{ns: {"publish", "consume"}},
		Metadata:    map[string]string{"picture": userInfo.Picture},
	}

	return id, "", nil
}

func (o *OAuthProvider) GetAuthorizationURL(state string) string {
	baseURL := o.getAuthURL()
	params := url.Values{}
	params.Set("client_id", o.config.ClientID)
	params.Set("redirect_uri", o.config.RedirectURL)
	params.Set("response_type", "code")
	params.Set("scope", joinScopes(o.config.Scopes))
	params.Set("state", state)

	return baseURL + "?" + params.Encode()
}

func (o *OAuthProvider) ExchangeCode(ctx context.Context, code string) (*TokenResponse, error) {
	data := url.Values{}
	data.Set("client_id", o.config.ClientID)
	data.Set("client_secret", o.config.ClientSecret)
	data.Set("code", code)
	data.Set("grant_type", "authorization_code")
	data.Set("redirect_uri", o.config.RedirectURL)

	req, err := http.NewRequestWithContext(ctx, "POST", o.getTokenURL(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Body = io.NopCloser(io.NopCloser(nil))

	resp, err := o.client.PostForm(o.getTokenURL(), data)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token exchange failed: %d", resp.StatusCode)
	}

	var token TokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return nil, err
	}

	return &token, nil
}

func (o *OAuthProvider) validateGoogleToken(ctx context.Context, token string) (*UserInfo, error) {
	// In production, validate with Google's tokeninfo endpoint
	// For now, this is a placeholder
	return &UserInfo{
		ID:    "google-" + token[:8],
		Email: "user@example.com",
		Name:  "Google User",
	}, nil
}

func (o *OAuthProvider) validateGitHubToken(ctx context.Context, token string) (*UserInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", "https://api.github.com/user", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := o.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GitHub API error: %d", resp.StatusCode)
	}

	var userInfo UserInfo
	if err := json.NewDecoder(resp.Body).Decode(&userInfo); err != nil {
		return nil, err
	}

	return &userInfo, nil
}

func (o *OAuthProvider) validateMicrosoftToken(ctx context.Context, token string) (*UserInfo, error) {
	return &UserInfo{
		ID:    "microsoft-" + token[:8],
		Email: "user@outlook.com",
		Name:  "Microsoft User",
	}, nil
}

func (o *OAuthProvider) getAuthURL() string {
	switch o.config.Provider {
	case "google":
		return "https://accounts.google.com/o/oauth2/v2/auth"
	case "github":
		return "https://github.com/login/oauth/authorize"
	case "microsoft":
		return "https://login.microsoftonline.com/common/oauth2/v2.0/authorize"
	default:
		return ""
	}
}

func (o *OAuthProvider) getTokenURL() string {
	switch o.config.Provider {
	case "google":
		return "https://oauth2.googleapis.com/token"
	case "github":
		return "https://github.com/login/oauth/access_token"
	case "microsoft":
		return "https://login.microsoftonline.com/common/oauth2/v2.0/token"
	default:
		return ""
	}
}

func (o *OAuthProvider) mapTeamToNamespace(userInfo *UserInfo) string {
	for _, team := range userInfo.Teams {
		if ns, ok := o.config.TeamsMap[team]; ok {
			return ns
		}
	}
	return "default"
}

func joinScopes(scopes []string) string {
	result := ""
	for i, s := range scopes {
		if i > 0 {
			result += " "
		}
		result += s
	}
	return result
}

func GenerateState() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}
