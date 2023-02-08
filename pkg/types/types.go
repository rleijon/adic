package types

type FieldType uint16

const (
	IntType FieldType = iota
	StringType
)

type Field struct {
	Type FieldType
	Name string
}

type Object struct {
	Values map[string]interface{}
}
