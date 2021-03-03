package index

import (
	"fmt"
	"reflect"
	"strings"
)

// StringSliceFieldIndex builds an index from a field on an object that is a
// string slice ([]string). Each value within the string slice can be used for
// lookup.
type StringSliceFieldIndex struct {
	Field     string
	Lowercase bool
}

func (s *StringSliceFieldIndex) FromObject(obj interface{}) (bool, [][]byte, error) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v) // Dereference the pointer if any

	fv := v.FieldByName(s.Field)
	if !fv.IsValid() {
		return false, nil,
			fmt.Errorf("field '%s' for %#v is invalid", s.Field, obj)
	}

	if fv.Kind() != reflect.Slice || fv.Type().Elem().Kind() != reflect.String {
		return false, nil, fmt.Errorf("field '%s' is not a string slice", s.Field)
	}

	length := fv.Len()
	vals := make([][]byte, 0, length)
	for i := 0; i < fv.Len(); i++ {
		val := fv.Index(i).String()
		if val == "" {
			continue
		}

		if s.Lowercase {
			val = strings.ToLower(val)
		}

		// Add the null character as a terminator
		val += "\x00"
		vals = append(vals, []byte(val))
	}
	if len(vals) == 0 {
		return false, nil, nil
	}
	return true, vals, nil
}

func (s *StringSliceFieldIndex) FromArgs(args ...interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}
	arg, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("argument must be a string: %#v", args[0])
	}
	if s.Lowercase {
		arg = strings.ToLower(arg)
	}
	// Add the null character as a terminator
	arg += "\x00"
	return []byte(arg), nil
}

func (s *StringSliceFieldIndex) PrefixFromArgs(args ...interface{}) ([]byte, error) {
	val, err := s.FromArgs(args...)
	if err != nil {
		return nil, err
	}

	// Strip the null terminator, the rest is a prefix
	n := len(val)
	if n > 0 {
		return val[:n-1], nil
	}
	return val, nil
}
