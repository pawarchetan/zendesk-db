package index

import (
	"fmt"
	"reflect"
)

// BoolFieldIndex is used to extract an boolean field from an object using
// reflection and builds an index on that field.
type BoolFieldIndex struct {
	Field string
}

func (i *BoolFieldIndex) FromObject(obj interface{}) (bool, []byte, error) {
	v := reflect.ValueOf(obj)
	v = reflect.Indirect(v) // Dereference the pointer if any

	fv := v.FieldByName(i.Field)
	if !fv.IsValid() {
		return false, nil,
			fmt.Errorf("field '%s' for %#v is invalid", i.Field, obj)
	}

	// Check the type
	k := fv.Kind()
	if k != reflect.Bool {
		return false, nil, fmt.Errorf("field %q is of type %v; want a bool", i.Field, k)
	}

	buf := make([]byte, 1)
	if fv.Bool() {
		buf[0] = 1
	}

	return true, buf, nil
}

func (i *BoolFieldIndex) FromArgs(args ...interface{}) ([]byte, error) {
	return fromBoolArgs(args)
}

// fromBoolArgs is a helper that expects only a single boolean argument and
// returns a single length byte array containing either a one or zero depending
// on whether the passed input is true or false.
func fromBoolArgs(args []interface{}) ([]byte, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("must provide only a single argument")
	}

	if val, ok := args[0].(bool); !ok {
		return nil, fmt.Errorf("argument must be a boolean type: %#v", args[0])
	} else if val {
		return []byte{1}, nil
	}

	return []byte{0}, nil
}

