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
