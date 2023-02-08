package writer

type Filter struct {
	Columns []ColumnFilter
}

type ColumnFilter interface {
	ColumnName() string
	Test(interface{}) bool
}

type EqualFilter struct {
	Name  string
	Value interface{}
}

func (f *EqualFilter) ColumnName() string {
	return f.Name
}

func (f *EqualFilter) Test(v interface{}) bool {
	return f.Value == v
}
