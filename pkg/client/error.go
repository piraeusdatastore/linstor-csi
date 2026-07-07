package client

import "fmt"

type DeleteInProgressError struct {
	Kind      string
	Name      string
	Operation string
}

func (d *DeleteInProgressError) Error() string {
	return fmt.Sprintf("'%s' failed, %s %s is being deleted", d.Operation, d.Kind, d.Name)
}

// ResourceGroupConflictError reports that a resource definition already belongs to a different resource group
// than requested — for a consistency group, a member's StorageClass disagreeing with the group's.
type ResourceGroupConflictError struct {
	Resource string
	Existing string
	Wanted   string
}

func (e *ResourceGroupConflictError) Error() string {
	return fmt.Sprintf("resource definition %q already belongs to resource group %q, but %q was requested", e.Resource, e.Existing, e.Wanted)
}
