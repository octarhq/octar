package v1

import (
	"context"
	"net/http"

	"github.com/danielgtaylor/huma/v2"

	"github.com/83codes/octar/internal/db"
)

// userView is the public representation of a user — never exposes password_hash.
type userView struct {
	ID        int    `json:"id"`
	Username  string `json:"username"`
	Email     *string `json:"email,omitempty"`
	Role      string `json:"role"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at,omitempty"`
}

func toUserView(u db.User) userView {
	return userView{
		ID:        u.ID,
		Username:  u.Username,
		Email:     u.Email,
		Role:      u.Role,
		CreatedAt: u.CreatedAt,
		UpdatedAt: u.UpdatedAt,
	}
}

func registerUsers(api huma.API, store *db.Store) {
	type userPath struct {
		Username string `path:"username" doc:"Username"`
	}

	// GET /users
	huma.Register(api, huma.Operation{
		OperationID: "list-users",
		Method:      http.MethodGet,
		Path:        "/users",
		Summary:     "List users",
		Description: "Returns all operator accounts registered in the broker.",
		Tags:        []string{"users"},
		Security:    bearerAuth,
	}, func(ctx context.Context, _ *struct{}) (*struct{ Body []userView }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		list, err := store.ListUsers()
		if err != nil {
			return nil, err
		}
		views := make([]userView, len(list))
		for i, u := range list {
			views[i] = toUserView(u)
		}
		return &struct{ Body []userView }{Body: views}, nil
	})

	// POST /users
	type createUserInput struct {
		Body struct {
			Username string `json:"username" minLength:"3" maxLength:"64" pattern:"^[a-z0-9_-]+$" doc:"Unique login name"`
			Password string `json:"password" minLength:"8" doc:"Plaintext password (hashed before storage)"`
			Email    string `json:"email" format:"email" doc:"Contact email address"`
			Role     string `json:"role" enum:"admin,producer,consumer,observer,billing,service" doc:"RBAC role"`
		}
	}
	huma.Register(api, huma.Operation{
		OperationID:   "create-user",
		Method:        http.MethodPost,
		Path:          "/users",
		Summary:       "Create a user",
		Description:   "Creates a new operator account. Passwords are bcrypt-hashed before storage.",
		Tags:          []string{"users"},
		DefaultStatus: http.StatusCreated,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *createUserInput) (*struct{ Body userView }, error) {
		id, err := requireAuth(ctx)
		if err != nil {
			return nil, err
		}
		if !id.IsAdmin() && !id.CanManageUsers() {
			return nil, huma.Error403Forbidden("insufficient permissions")
		}
		if err := store.CreateUser(
			input.Body.Username,
			input.Body.Password,
			input.Body.Email,
			input.Body.Role,
		); err != nil {
			return nil, dbError(err, "user")
		}
		u, _ := store.GetUser(input.Body.Username)
		return &struct{ Body userView }{Body: toUserView(*u)}, nil
	})

	// GET /users/{username}
	huma.Register(api, huma.Operation{
		OperationID: "get-user",
		Method:      http.MethodGet,
		Path:        "/users/{username}",
		Summary:     "Get a user",
		Description: "Returns a single user by username.",
		Tags:        []string{"users"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *userPath) (*struct{ Body userView }, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		u, err := store.GetUser(input.Username)
		if err != nil {
			return nil, dbError(err, "user")
		}
		return &struct{ Body userView }{Body: toUserView(*u)}, nil
	})

	// PATCH /users/{username}
	type patchUserInput struct {
		Username string `path:"username" doc:"Username"`
		Body     struct {
			Email    *string `json:"email,omitempty" format:"email" doc:"New email address"`
			Role     *string `json:"role,omitempty" enum:"admin,producer,consumer,observer,billing,service" doc:"New RBAC role"`
			Password *string `json:"password,omitempty" minLength:"8" doc:"New password (will be hashed)"`
		}
	}
	huma.Register(api, huma.Operation{
		OperationID: "update-user",
		Method:      http.MethodPatch,
		Path:        "/users/{username}",
		Summary:     "Update a user",
		Description: "Partially updates a user. Only the fields provided are changed.",
		Tags:        []string{"users"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *patchUserInput) (*struct{ Body userView }, error) {
		id, err := requireAuth(ctx)
		if err != nil {
			return nil, err
		}
		if !id.IsAdmin() && !id.CanManageUsers() && id.SubjectID != input.Username {
			return nil, huma.Error403Forbidden("insufficient permissions")
		}
		if err := store.UpdateUser(input.Username, input.Body.Email, input.Body.Role, input.Body.Password); err != nil {
			return nil, dbError(err, "user")
		}
		u, _ := store.GetUser(input.Username)
		return &struct{ Body userView }{Body: toUserView(*u)}, nil
	})

	// DELETE /users/{username}
	huma.Register(api, huma.Operation{
		OperationID:   "delete-user",
		Method:        http.MethodDelete,
		Path:          "/users/{username}",
		Summary:       "Delete a user",
		Description:   "Permanently removes a user account.",
		Tags:          []string{"users"},
		DefaultStatus: http.StatusNoContent,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *userPath) (*struct{}, error) {
		id, err := requireAuth(ctx)
		if err != nil {
			return nil, err
		}
		if !id.IsAdmin() && !id.CanManageUsers() {
			return nil, huma.Error403Forbidden("insufficient permissions")
		}
		if err := store.DeleteUser(input.Username); err != nil {
			return nil, dbError(err, "user")
		}
		return nil, nil
	})

	// GET /users/{username}/permissions
	type userPermissionsOutput struct {
		Body struct {
			Username    string              `json:"username"`
			Permissions map[string][]string `json:"permissions" doc:"Map of namespace → permission list"`
		}
	}
	huma.Register(api, huma.Operation{
		OperationID: "get-user-permissions",
		Method:      http.MethodGet,
		Path:        "/users/{username}/permissions",
		Summary:     "Get user namespace permissions",
		Description: "Returns all per-namespace permissions granted to a user.",
		Tags:        []string{"users"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *userPath) (*userPermissionsOutput, error) {
		if _, err := requireAuth(ctx); err != nil {
			return nil, err
		}
		perms, err := store.GetUserNamespacePermissionsByUsername(input.Username)
		if err != nil {
			return nil, dbError(err, "user")
		}
		out := &userPermissionsOutput{}
		out.Body.Username = input.Username
		out.Body.Permissions = perms
		return out, nil
	})

	// PUT /users/{username}/permissions/{namespace}
	type setPermissionsInput struct {
		Username  string `path:"username" doc:"Username"`
		Namespace string `path:"namespace" doc:"Namespace name"`
		Body      struct {
			Permissions []string `json:"permissions" doc:"List of permission strings to grant. Use '*' for all." minItems:"1"`
		}
	}
	type userSetPermissionsOutput struct {
		Body struct {
			Username    string   `json:"username"`
			Namespace   string   `json:"namespace"`
			Permissions []string `json:"permissions"`
		}
	}
	huma.Register(api, huma.Operation{
		OperationID: "set-user-namespace-permissions",
		Method:      http.MethodPut,
		Path:        "/users/{username}/permissions/{namespace}",
		Summary:     "Set user permissions for a namespace",
		Description: "Replaces all permissions for a user within a specific namespace. Use `GET /permissions` to see valid permission values.",
		Tags:        []string{"users"},
		Security:    bearerAuth,
	}, func(ctx context.Context, input *setPermissionsInput) (*userSetPermissionsOutput, error) {
		id, err := requireAuth(ctx)
		if err != nil {
			return nil, err
		}
		if !id.IsAdmin() && !id.CanManageUsers() {
			return nil, huma.Error403Forbidden("insufficient permissions")
		}
		if err := store.SetUserNamespacePermissionsByName(
			input.Username, input.Namespace, input.Body.Permissions,
		); err != nil {
			return nil, dbError(err, "user or namespace")
		}
		out := &userSetPermissionsOutput{}
		out.Body.Username = input.Username
		out.Body.Namespace = input.Namespace
		out.Body.Permissions = input.Body.Permissions
		return out, nil
	})

	// DELETE /users/{username}/permissions/{namespace}
	type deletePermissionsInput struct {
		Username  string `path:"username" doc:"Username"`
		Namespace string `path:"namespace" doc:"Namespace name"`
	}
	huma.Register(api, huma.Operation{
		OperationID:   "delete-user-namespace-permissions",
		Method:        http.MethodDelete,
		Path:          "/users/{username}/permissions/{namespace}",
		Summary:       "Revoke user permissions for a namespace",
		Description:   "Removes all namespace-scoped permissions for the user in the specified namespace.",
		Tags:          []string{"users"},
		DefaultStatus: http.StatusNoContent,
		Security:      bearerAuth,
	}, func(ctx context.Context, input *deletePermissionsInput) (*struct{}, error) {
		id, err := requireAuth(ctx)
		if err != nil {
			return nil, err
		}
		if !id.IsAdmin() && !id.CanManageUsers() {
			return nil, huma.Error403Forbidden("insufficient permissions")
		}
		// Pass empty slice to effectively revoke all permissions.
		if err := store.SetUserNamespacePermissionsByName(input.Username, input.Namespace, []string{}); err != nil {
			return nil, dbError(err, "user or namespace")
		}
		return nil, nil
	})
}
