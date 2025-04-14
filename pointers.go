package shiva

import (
	"reflect"
)

// StringPtr returns a pointer to the string passed in. This is useful since
// the official confluent-kafka-go library uses pointers to strings in many
// places like topics and Go doesn't allow you to take the address of a string
// in a single statement.
func StringPtr(s string) *string {
	return &s
}

// Ptr returns a pointer to the instance passed in.
func Ptr[T any](v T) *T {
	if v == nil {
		return nil
	}
	return &v
}

// IsNil checks if the v is nil
//
// IsNil is useful as it handles the gotcha case of interfaces where an interface
// is only considered nil if both the type and value are nil. If the type is known
// but the value is nil a simple `if v != nil` of `if v == nil` check can result
// in unexpected results.
func IsNil(v any) bool {
	if v == nil {
		return true
	}

	val := reflect.ValueOf(v)
	kind := val.Kind()

	switch kind {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return val.IsNil()
	default:
		return false
	}
}
