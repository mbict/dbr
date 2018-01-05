package dbr

// XxxBuilders all support raw query
type raw struct {
	query string
	value []interface{}
}

// Expr should be used when sql syntax is not supported
func Expr(query string, value ...interface{}) Builder {
	return &raw{query: query, value: value}
}

func (raw *raw) Build(_ Dialect, buf Buffer) error {
	buf.WriteString(raw.query)
	buf.WriteValue(raw.value...)
	return nil
}
