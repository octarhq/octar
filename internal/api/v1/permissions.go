package v1

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"
)

// PermissionInfo describes a single permission string for the API catalog.
type PermissionInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// GlobalPermissionCatalog lists broker-wide permissions.
var GlobalPermissionCatalog = []PermissionInfo{
	{Name: "admin", Description: "Full access to all broker operations"},
	{Name: "publish", Description: "Publish messages to any queue"},
	{Name: "consume", Description: "Consume messages from any queue"},
	{Name: "ack", Description: "Acknowledge messages"},
	{Name: "nack", Description: "Negative-acknowledge messages"},
	{Name: "queues:manage", Description: "Declare, configure, and delete queues"},
	{Name: "users:manage", Description: "Create, update, and delete users"},
	{Name: "metrics:read", Description: "Read broker metrics"},
}

// NamespacePermissionCatalog lists per-namespace permissions.
var NamespacePermissionCatalog = []PermissionInfo{
	{Name: "publish", Description: "Publish messages to queues in this namespace"},
	{Name: "consume", Description: "Consume messages from queues in this namespace"},
	{Name: "ack", Description: "Acknowledge messages in this namespace"},
	{Name: "nack", Description: "Negative-acknowledge messages in this namespace"},
	{Name: "queues:manage", Description: "Declare and configure queues in this namespace"},
}

func registerPermissions(api huma.API) {
	type permissionsOutput struct {
		Body struct {
			Global    []PermissionInfo `json:"global"`
			Namespace []PermissionInfo `json:"namespace"`
		}
	}

	huma.Register(api, huma.Operation{
		OperationID: "list-permissions",
		Method:      http.MethodGet,
		Path:        "/permissions",
		Summary:     "List all available permission values",
		Description: "Returns the catalog of valid permission strings for global and namespace scopes. Use '*' to grant all permissions in a given scope.",
		Tags:        []string{"system"},
	}, func(_ context.Context, _ *struct{}) (*permissionsOutput, error) {
		out := &permissionsOutput{}
		out.Body.Global = GlobalPermissionCatalog
		out.Body.Namespace = NamespacePermissionCatalog
		return out, nil
	})
}
