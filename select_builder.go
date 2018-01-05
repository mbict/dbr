package dbr

import (
	"context"
	"database/sql"
)

type SelectBuilder struct {
	runner        runner
	eventReceiver EventReceiver
	Dialect       Dialect

	*SelectStmt
}

func prepareSelect(a []string) []interface{} {
	b := make([]interface{}, len(a))
	for i := range a {
		b[i] = a[i]
	}
	return b
}

func (sess *Session) Select(column ...string) *SelectBuilder {
	return &SelectBuilder{
		runner:        sess,
		eventReceiver: sess.EventReceiver,
		Dialect:       sess.Dialect,
		SelectStmt:    Select(prepareSelect(column)...),
	}
}

func (tx *Tx) Select(column ...string) *SelectBuilder {
	return &SelectBuilder{
		runner:        tx,
		eventReceiver: tx.EventReceiver,
		Dialect:       tx.Dialect,
		SelectStmt:    Select(prepareSelect(column)...),
	}
}

func (sess *Session) SelectBySql(query string, value ...interface{}) *SelectBuilder {
	return &SelectBuilder{
		runner:        sess,
		eventReceiver: sess.EventReceiver,
		Dialect:       sess.Dialect,
		SelectStmt:    SelectBySql(query, value...),
	}
}

func (tx *Tx) SelectBySql(query string, value ...interface{}) *SelectBuilder {
	return &SelectBuilder{
		runner:        tx,
		eventReceiver: tx.EventReceiver,
		Dialect:       tx.Dialect,
		SelectStmt:    SelectBySql(query, value...),
	}
}

func (b *SelectBuilder) LoadOneContext(ctx context.Context, value interface{}) error {
	count, err := query(ctx, b.runner, b.eventReceiver, b, b.Dialect, value)
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	return nil
}

func (b *SelectBuilder) LoadOne(value interface{}) error {
	return b.LoadOneContext(context.Background(), value)
}

func (b *SelectBuilder) LoadContext(ctx context.Context, value interface{}) (int, error) {
	return query(ctx, b.runner, b.eventReceiver, b, b.Dialect, value)
}

func (b *SelectBuilder) Load(value interface{}) (int, error) {
	return b.LoadContext(context.Background(), value)
}

func (b *SelectBuilder) QueryContext(ctx context.Context) (*sql.Rows, error) {
	return queryRows(ctx, b.runner, b.eventReceiver, b, b.Dialect)
}

func (b *SelectBuilder) Query() (*sql.Rows, error) {
	return b.QueryContext(context.Background())
}

func (b *SelectBuilder) Join(table, on interface{}) *SelectBuilder {
	b.SelectStmt.Join(table, on)
	return b
}

func (b *SelectBuilder) LeftJoin(table, on interface{}) *SelectBuilder {
	b.SelectStmt.LeftJoin(table, on)
	return b
}

func (b *SelectBuilder) RightJoin(table, on interface{}) *SelectBuilder {
	b.SelectStmt.RightJoin(table, on)
	return b
}

func (b *SelectBuilder) FullJoin(table, on interface{}) *SelectBuilder {
	b.SelectStmt.FullJoin(table, on)
	return b
}

func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.SelectStmt.Distinct()
	return b
}

func (b *SelectBuilder) From(table interface{}) *SelectBuilder {
	b.SelectStmt.From(table)
	return b
}

func (b *SelectBuilder) GroupBy(col ...string) *SelectBuilder {
	b.SelectStmt.GroupBy(col...)
	return b
}

func (b *SelectBuilder) Having(query interface{}, value ...interface{}) *SelectBuilder {
	b.SelectStmt.Having(query, value...)
	return b
}

func (b *SelectBuilder) Limit(n uint64) *SelectBuilder {
	b.SelectStmt.Limit(n)
	return b
}

func (b *SelectBuilder) Offset(n uint64) *SelectBuilder {
	b.SelectStmt.Offset(n)
	return b
}

func (b *SelectBuilder) OrderDir(col string, isAsc bool) *SelectBuilder {
	if isAsc {
		b.SelectStmt.OrderAsc(col)
	} else {
		b.SelectStmt.OrderDesc(col)
	}
	return b
}

// OrderBy specifies columns for ordering
func (b *SelectBuilder) OrderAsc(col string) *SelectBuilder {
	b.OrderDir(col, true)
	return b
}

func (b *SelectBuilder) OrderDesc(col string) *SelectBuilder {
	b.OrderDir(col, false)
	return b
}

func (b *SelectBuilder) Paginate(page, perPage uint64) *SelectBuilder {
	b.Limit(perPage)
	b.Offset((page - 1) * perPage)
	return b
}

func (b *SelectBuilder) OrderBy(col string) *SelectBuilder {
	b.SelectStmt.Order = append(b.SelectStmt.Order, Expr(col))
	return b
}

func (b *SelectBuilder) Where(query interface{}, value ...interface{}) *SelectBuilder {
	b.SelectStmt.Where(query, value...)
	return b
}
