package clean

// Cursor tracks the position of a scanning pass so batches never overlap and
// never skip a tail remainder.
type Cursor struct {
	pos   int
	total int
}

// NewCursor starts a scan over total entries.
func NewCursor(total int) *Cursor {
	return &Cursor{total: total}
}

// Position returns the current cursor offset.
func (c *Cursor) Position() int {
	return c.pos
}

// Remaining reports how many entries are still unvisited.
func (c *Cursor) Remaining() int {
	if c.pos >= c.total {
		return 0
	}
	return c.total - c.pos
}

// Advance moves the cursor by count entries.
func (c *Cursor) Advance(count int) {
	c.pos += count
	if c.pos > c.total {
		c.pos = c.total
	}
}
