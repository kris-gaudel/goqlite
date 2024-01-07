package cursor

import (
	"github.com/kris-gaudel/goqlite/constants"
	"github.com/kris-gaudel/goqlite/pager"
	"github.com/kris-gaudel/goqlite/table"
)

type Cursor struct {
	Table      *table.Table
	RowNum     uint32
	EndOfTable bool
}

func TableStart(tableInstance *table.Table) *Cursor {
	cursor := &Cursor{Table: tableInstance, RowNum: 0}
	cursor.EndOfTable = (tableInstance.NumRows == 0)
	return cursor
}

func TableEnd(table *table.Table) *Cursor {
	cursor := &Cursor{Table: table, RowNum: table.NumRows, EndOfTable: true}
	return cursor
}

func CursorValue(cursor *Cursor) []byte {
	rowNum := cursor.RowNum
	pageNum := rowNum / constants.ROWS_PER_PAGE
	page := pager.GetPage(cursor.Table.Pager, pageNum)
	rowOffset := rowNum % constants.ROWS_PER_PAGE
	byteOffset := rowOffset * constants.ROW_SIZE

	return page[byteOffset:]
}

func CursorAdvance(cursor *Cursor) {
	cursor.RowNum += 1
	if cursor.RowNum >= cursor.Table.NumRows {
		cursor.EndOfTable = true
	}
}
