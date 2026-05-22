package v1

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/83codes/octar/internal/auth"
)

func registerAPIKeys(api huma.API, authSvc *auth.Service) {
	type createInput struct {
		Body struct {
			Name        string   `json:"name"`
			Namespace   string   `json:"namespace"`
			Permissions []string `json:"permissions"`
		}
	}

	type createOutput struct {
		Body struct {
			Key         string   `json:"key"`
			SubjectID   string   `json:"subject_id"`
			Namespace   string   `json:"namespace"`
			Permissions []string `json:"permissions"`
		}
	}

	huma.Register(api, huma.Operation{
		OperationID: "create-api-key",
		Method:      http.MethodPost,
		Path:        "/auth/api-keys",
		Summary:     "Create API key",
		Description: "Creates a long-lived API key scoped to a namespace. The plaintext key is returned once and never stored.",
		Tags:        []string{"api-keys"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *createInput) (*createOutput, error) {
		id := identityFrom(ctx)
		if id == nil {
			return nil, huma.Error401Unauthorized("unauthorized")
		}
		if !id.CanManageUsers() && !id.IsAdmin() {
			return nil, huma.Error403Forbidden("insufficient permissions")
		}

		permissions := input.Body.Permissions
		if len(permissions) == 0 {
			permissions = []string{"publish", "consume"}
		}

		key := authSvc.GenerateAPIKey(input.Body.Name, input.Body.Namespace, permissions)

		out := &createOutput{}
		out.Body.Key = key
		out.Body.SubjectID = input.Body.Name
		out.Body.Namespace = input.Body.Namespace
		out.Body.Permissions = permissions
		return out, nil
	})

	type listEntry struct {
		SubjectID   string   `json:"subject_id"`
		Namespace   string   `json:"namespace"`
		Permissions []string `json:"permissions"`
		Prefix      string   `json:"prefix,omitempty"`
	}

	huma.Register(api, huma.Operation{
		OperationID: "list-api-keys",
		Method:      http.MethodGet,
		Path:        "/auth/api-keys",
		Summary:     "List API keys",
		Description: "Returns all active API keys. Plaintext keys are never stored — only hashes and metadata are returned.",
		Tags:        []string{"api-keys"},
		Security:    bearerAuth,
	}, func(ctx context.Context, _ *struct{}) (*struct{ Body []listEntry }, error) {
		id := identityFrom(ctx)
		if id == nil {
			return nil, huma.Error401Unauthorized("unauthorized")
		}
		if !id.CanManageUsers() && !id.IsAdmin() {
			return nil, huma.Error403Forbidden("insufficient permissions")
		}

		keys := authSvc.ListAPIKeys()
		entries := make([]listEntry, len(keys))
		for i, k := range keys {
			entries[i] = listEntry{
				SubjectID:   k.SubjectID,
				Namespace:   k.Namespace,
				Permissions: k.Permissions,
				Prefix:      k.Prefix,
			}
		}
		return &struct{ Body []listEntry }{Body: entries}, nil
	})

	type revokeInput struct {
		Body struct {
			Key string `json:"key"`
		}
	}

	type revokeOutput struct {
		Body struct {
			Success bool `json:"success"`
		}
	}

	huma.Register(api, huma.Operation{
		OperationID: "revoke-api-key",
		Method:      http.MethodDelete,
		Path:        "/auth/api-keys",
		Summary:     "Revoke API key",
		Description: "Permanently revokes an API key. Active sessions using this key will be rejected on the next request.",
		Tags:        []string{"api-keys"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *revokeInput) (*revokeOutput, error) {
		id := identityFrom(ctx)
		if id == nil {
			return nil, huma.Error401Unauthorized("unauthorized")
		}
		if !id.CanManageUsers() && !id.IsAdmin() {
			return nil, huma.Error403Forbidden("insufficient permissions")
		}

		authSvc.RevokeAPIKey(input.Body.Key)
		out := &revokeOutput{}
		out.Body.Success = true
		return out, nil
	})
}
