package host

import "context"

// Readiness reports whether the Host Provider can currently serve host data.
//
// Returned error messages may be exposed through the provider Health RPC and
// therefore must not contain credentials or other sensitive information.
type Readiness interface {
	Ready(context.Context) error
}

// ReadinessFunc adapts a function to Readiness.
type ReadinessFunc func(context.Context) error

func (f ReadinessFunc) Ready(ctx context.Context) error {
	return f(ctx)
}
