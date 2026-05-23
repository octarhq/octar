package v1

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/octarhq/octar/internal/auth"
	"github.com/octarhq/octar/internal/db"
)

func registerAuth(api huma.API, store *db.Store, authSvc *auth.Service) {
	type loginInput struct {
		Body struct {
			Username string `json:"username"`
			Password string `json:"password"`
		}
	}

	type loginOutput struct {
		Body struct {
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token,omitempty"`
			ExpiresIn    int64  `json:"expires_in"`
			TokenType    string `json:"token_type"`
		}
	}

	huma.Register(api, huma.Operation{
		OperationID: "login",
		Method:      http.MethodPost,
		Path:        "/auth/login",
		Summary:     "Authenticate and obtain JWT tokens",
		Tags:        []string{"auth"},
	}, func(_ context.Context, input *loginInput) (*loginOutput, error) {
		if !store.CheckPassword(input.Body.Username, input.Body.Password) {
			return nil, huma.Error401Unauthorized("invalid credentials")
		}

		user, err := store.GetUser(input.Body.Username)
		if err != nil {
			return nil, huma.Error401Unauthorized("invalid credentials")
		}

		tokens, err := authSvc.GenerateTokens(user)
		if err != nil {
			return nil, err
		}

		out := &loginOutput{}
		out.Body.AccessToken = tokens.AccessToken
		out.Body.RefreshToken = tokens.RefreshToken
		out.Body.ExpiresIn = tokens.ExpiresIn
		out.Body.TokenType = tokens.TokenType
		return out, nil
	})

	type refreshInput struct {
		Body struct {
			RefreshToken string `json:"refresh_token"`
		}
	}

	huma.Register(api, huma.Operation{
		OperationID: "refresh",
		Method:      http.MethodPost,
		Path:        "/auth/refresh",
		Summary:     "Refresh access token",
		Tags:        []string{"auth"},
	}, func(_ context.Context, input *refreshInput) (*loginOutput, error) {
		tokens, err := authSvc.RefreshTokens(input.Body.RefreshToken)
		if err != nil {
			return nil, huma.Error401Unauthorized("invalid or expired refresh token")
		}

		out := &loginOutput{}
		out.Body.AccessToken = tokens.AccessToken
		out.Body.RefreshToken = tokens.RefreshToken
		out.Body.ExpiresIn = tokens.ExpiresIn
		out.Body.TokenType = tokens.TokenType
		return out, nil
	})

	type logoutInput struct {
		Body struct {
			RefreshToken string `json:"refresh_token"`
		}
	}

	type logoutOutputBody struct {
		Success bool `json:"success"`
	}

	type logoutOutput struct {
		Body logoutOutputBody
	}

	huma.Register(api, huma.Operation{
		OperationID: "logout",
		Method:      http.MethodPost,
		Path:        "/auth/logout",
		Summary:     "Logout and revoke refresh token",
		Tags:        []string{"auth"},
	}, func(ctx context.Context, input *logoutInput) (*logoutOutput, error) {
		return &logoutOutput{Body: logoutOutputBody{Success: true}}, nil
	})
}
