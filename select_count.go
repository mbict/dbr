package dbr

import "context"

// CountContext creates a wrapper around the original query and counts creates a sum of the sub select
// This class should be used with complex queries who use grouping functions
func (b *SelectStmt) CountContext(ctx context.Context) (uint64, error) {
	b = b.Select("SUM(1) as count")
	b.LimitCount = -1
	b.OffsetCount = -1

	buf := NewBuffer()
	if err := b.Build(b.Dialect, buf); err != nil {
		return 0, err
	}

	b = SelectBySql("SELECT SUM(count) FROM (" + buf.String() + ")")

	var c uint64
	err := b.LoadOneContext(ctx, &c)
	return c, err
}

func (b *SelectStmt) Count() (uint64, error) {
	return b.CountContext(context.Background())
}

//SimpleCountContext alters the columns of the original query to a count grouping function
//it will also remove the group by if it exists
func (b *SelectStmt) SimpleCountContext(ctx context.Context) (uint64, error) {
	b = b.Select("COUNT(*)")
	b.LimitCount = -1
	b.OffsetCount = -1
	b.Group = []Builder{}

	var count uint64
	_, err := query(ctx, b.runner, b.EventReceiver, b, b.Dialect, &count)
	return count, err
}

func (b *SelectStmt) SimpleCount() (uint64, error) {
	return b.CountContext(context.Background())
}
