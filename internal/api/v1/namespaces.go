package v1

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/octarhq/octar/internal/auth"
	"github.com/octarhq/octar/internal/db"
)

// namespaceView is the public representation of a namespace (without raw config blob).
type namespaceView struct {
	ID        int            `json:"id"`
	Name      string         `json:"name"`
	Config    map[string]any `json:"config,omitempty"`
	CreatedAt string         `json:"created_at"`
	UpdatedAt string         `json:"updated_at"`
}

func toNamespaceView(ns db.Namespace) namespaceView {
	v := namespaceView{
		ID:        ns.ID,
		Name:      ns.Name,
		CreatedAt: ns.CreatedAt,
		UpdatedAt: ns.UpdatedAt,
	}
	if ns.Config != "" && ns.Config != "{}" {
		var m map[string]any
		if json.Unmarshal([]byte(ns.Config), &m) == nil {
			v.Config = m
		}
	}
	return v
}

func registerNamespaces(api huma.API, store *db.Store, _ *auth.Service) {
	type nsPath struct {
		Name string `path:"name" doc:"Namespace name"`
	}

	// GET /namespaces
	huma.Register(api, huma.Operation{
		OperationID: "list-namespaces",
		Method:      http.MethodGet,
		Path:        "/namespaces",
		Summary:     "List namespaces",
		Description: "Returns all namespaces in the broker.",
		Tags:        []string{"namespaces"},
		Security:    bearerAuth,
	}, func(ctx context.Context, _ *struct{}) (*struct{ Body []namespaceView }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		list, err := store.ListNamespaces()
		if err != nil {
			return nil, err
		}
		views := make([]namespaceView, len(list))
		for i, ns := range list {
			views[i] = toNamespaceView(ns)
		}
		return &struct{ Body []namespaceView }{Body: views}, nil
	})

	// POST /namespaces
	type createNsInput struct {
		Body struct {
			Name   string         `json:"name" minLength:"1" maxLength:"64" pattern:"^[a-z0-9_-]+$" doc:"Unique namespace identifier"`
			Config map[string]any `json:"config,omitempty" doc:"Optional namespace-level configuration (reserved for future use)"`
		}
	}
	huma.Register(api, huma.Operation{
		OperationID:   "create-namespace",
		Method:        http.MethodPost,
		Path:          "/namespaces",
		Summary:       "Create a namespace",
		Description:   "Creates a new logical isolation boundary. Queues and groups belong to a namespace.",
		Tags:          []string{"namespaces"},
		DefaultStatus: http.StatusCreated,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *createNsInput) (*struct{ Body namespaceView }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		configJSON := "{}"
		if input.Body.Config != nil {
			b, _ := json.Marshal(input.Body.Config)
			configJSON = string(b)
		}
		ns, err := store.CreateNamespace(input.Body.Name, configJSON)
		if err != nil {
			return nil, dbError(err, "namespace")
		}
		return &struct{ Body namespaceView }{Body: toNamespaceView(*ns)}, nil
	})

	// GET /namespaces/{name}
	huma.Register(api, huma.Operation{
		OperationID: "get-namespace",
		Method:      http.MethodGet,
		Path:        "/namespaces/{name}",
		Summary:     "Get a namespace",
		Description: "Returns a single namespace by name.",
		Tags:        []string{"namespaces"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *nsPath) (*struct{ Body namespaceView }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		ns, err := store.GetNamespace(input.Name)
		if err != nil {
			return nil, dbError(err, "namespace")
		}
		return &struct{ Body namespaceView }{Body: toNamespaceView(*ns)}, nil
	})

	// PATCH /namespaces/{name}
	type patchNsInput struct {
		Name string `path:"name" doc:"Namespace name"`
		Body struct {
			Config map[string]any `json:"config,omitempty" doc:"Updated namespace configuration"`
		}
	}
	huma.Register(api, huma.Operation{
		OperationID: "update-namespace",
		Method:      http.MethodPatch,
		Path:        "/namespaces/{name}",
		Summary:     "Update a namespace",
		Description: "Updates the configuration of an existing namespace.",
		Tags:        []string{"namespaces"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *patchNsInput) (*struct{ Body namespaceView }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		configJSON := "{}"
		if input.Body.Config != nil {
			b, _ := json.Marshal(input.Body.Config)
			configJSON = string(b)
		}
		ns, err := store.UpdateNamespace(input.Name, configJSON)
		if err != nil {
			return nil, dbError(err, "namespace")
		}
		return &struct{ Body namespaceView }{Body: toNamespaceView(*ns)}, nil
	})

	// DELETE /namespaces/{name}
	huma.Register(api, huma.Operation{
		OperationID:   "delete-namespace",
		Method:        http.MethodDelete,
		Path:          "/namespaces/{name}",
		Summary:       "Delete a namespace",
		Description:   "Permanently deletes a namespace and all queues within it. Irreversible.",
		Tags:          []string{"namespaces"},
		DefaultStatus: http.StatusNoContent,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *nsPath) (*struct{}, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		if err := store.DeleteNamespace(input.Name); err != nil {
			return nil, dbError(err, "namespace")
		}
		return nil, nil
	})
}
