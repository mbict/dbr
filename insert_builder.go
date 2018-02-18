package dbr

import (
	"context"
	"database/sql"
	"reflect"
	"github.com/mbict/dbr/dialect"
)

type InsertBuilder struct {
	runner
	EventReceiver
	Dialect Dialect

	RecordID reflect.Value

	*InsertStmt
}

func (sess *Session) InsertInto(table string) *InsertBuilder {
	return &InsertBuilder{
		runner:        sess,
		EventReceiver: sess,
		Dialect:       sess.Dialect,
		InsertStmt:    InsertInto(table),
	}
}

func (tx *Tx) InsertInto(table string) *InsertBuilder {
	return &InsertBuilder{
		runner:        tx,
		EventReceiver: tx,
		Dialect:       tx.Dialect,
		InsertStmt:    InsertInto(table),
	}
}

func (sess *Session) InsertBySql(query string, value ...interface{}) *InsertBuilder {
	return &InsertBuilder{
		runner:        sess,
		EventReceiver: sess,
		Dialect:       sess.Dialect,
		InsertStmt:    InsertBySql(query, value...),
	}
}

func (tx *Tx) InsertBySql(query string, value ...interface{}) *InsertBuilder {
	return &InsertBuilder{
		runner:        tx,
		EventReceiver: tx,
		Dialect:       tx.Dialect,
		InsertStmt:    InsertBySql(query, value...),
	}
}

func (b *InsertBuilder) Pair(column string, value interface{}) *InsertBuilder {
	b.Column = append(b.Column, column)
	switch len(b.Value) {
	case 0:
		b.InsertStmt.Values(value)
	case 1:
		b.Value[0] = append(b.Value[0], value)
	default:
		panic("pair only allows one record to insert")
	}
	return b
}

func (b *InsertBuilder) Exec() (sql.Result, error) {
	return b.ExecContext(context.Background())
}

func (b *InsertBuilder) ExecContext(ctx context.Context) (sql.Result, error) {
	result, err := exec(ctx, b.runner, b.EventReceiver, b, b.Dialect)
	if err != nil {
		return nil, err
	}

	if b.RecordID.IsValid() {
		if id, err := result.LastInsertId(); err == nil {
			b.RecordID.SetInt(id)
		}
	}

	return result, nil
}

func (b *InsertBuilder) ExecId() (int64, error) {
	return b.ExecContextId(context.Background())
}

func (b *InsertBuilder) ExecContextId(ctx context.Context) (int64, error) {
	var (
		id  int64
		err error
	)
	switch b.Dialect {
	case dialect.PostgreSQL:
		err = b.Returning("id").LoadContext(ctx, &id)
	default:
		b.InsertStmt.Returning()
		r, err := b.ExecContext(ctx)
		if err == nil {
			id, err = r.LastInsertId()
		}
	}

	if err != nil {
		return 0, err
	}

	if b.RecordID.IsValid() {
		b.RecordID.SetInt(id)
	}

	return id, nil
}

func (b *InsertBuilder) LoadContext(ctx context.Context, value interface{}) error {
	_, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, value)
	return err
}

func (b *InsertBuilder) Load(value interface{}) error {
	return b.LoadContext(context.Background(), value)
}

func (b *InsertBuilder) Columns(column ...string) *InsertBuilder {
	b.InsertStmt.Columns(column...)
	return b
}

func (b *InsertBuilder) Returning(column ...string) *InsertBuilder {
	b.InsertStmt.Returning(column...)
	return b
}

func (b *InsertBuilder) Record(structValue interface{}) *InsertBuilder {
	v := reflect.Indirect(reflect.ValueOf(structValue))
	if v.Kind() == reflect.Struct && v.CanSet() {
		// ID is recommended by golint here
		for _, name := range []string{"Id", "ID"} {
			field := v.FieldByName(name)
			if field.IsValid() && (field.Kind() == reflect.Int64 || field.Kind() == reflect.Int) {
				b.RecordID = field
				break
			}
		}
	}

	b.InsertStmt.Record(structValue)
	return b
}

func (b *InsertBuilder) Values(value ...interface{}) *InsertBuilder {
	b.InsertStmt.Values(value...)
	return b
}
