package table

import (
	"encoding/binary"
	"fmt"
	"os"
	"strings"
	"syscall"

	"github.com/kris-gaudel/goqlite/constants"
	"github.com/kris-gaudel/goqlite/pager"
)

type Table struct {
	NumRows uint32
	Pager   *pager.Pager
}

type Row struct {
	Id       uint32
	Username [constants.COLUMN_USERNAME_SIZE + 1]rune
	Email    [constants.COLUMN_EMAIL_SIZE + 1]rune
}

func trimNullCharacters(input string) string {
	return strings.Trim(input, "\x00")
}

func PrintRow(row *Row) {
	stringifiedUsername := trimNullCharacters(string(row.Username[:]))
	stringifiedEmail := trimNullCharacters(string(row.Email[:]))
	fmt.Printf("(%d, %s, %s)\n", row.Id, stringifiedUsername, stringifiedEmail)
}

func SerializeRow(source *Row, destination *[]byte) {
	binary.LittleEndian.PutUint32((*destination)[constants.ID_OFFSET:constants.ID_OFFSET+constants.ID_SIZE], source.Id)
	copy((*destination)[constants.USERNAME_OFFSET:constants.USERNAME_OFFSET+constants.USERNAME_SIZE], []byte(trimNullCharacters(string(source.Username[:constants.USERNAME_SIZE]))))
	copy((*destination)[constants.EMAIL_OFFSET:constants.EMAIL_OFFSET+constants.EMAIL_SIZE], []byte(trimNullCharacters(string(source.Email[:constants.EMAIL_SIZE]))))
}

func DeserializeRow(source []byte, destination *Row) {
	destination.Id = binary.LittleEndian.Uint32(source[constants.ID_OFFSET : constants.ID_OFFSET+constants.ID_SIZE])
	copy(destination.Username[:], []rune(trimNullCharacters(string(source[constants.USERNAME_OFFSET:constants.USERNAME_OFFSET+constants.USERNAME_OFFSET]))))
	copy(destination.Email[:], []rune(trimNullCharacters(string(source[constants.EMAIL_OFFSET:constants.EMAIL_OFFSET+constants.EMAIL_SIZE]))))
}

func DBOpen(fileName string) *Table {
	pagerInstance := pager.PagerOpen(fileName)
	fmt.Println("File length: ", pagerInstance.FileLength)
	numRows := pagerInstance.FileLength / constants.ROW_SIZE
	fmt.Println("Num rows: ", numRows)

	table := &Table{NumRows: numRows, Pager: pagerInstance}
	return table
}

func DBClose(tableInstance *Table) {
	pagerInstance := tableInstance.Pager

	numFullPages := tableInstance.NumRows / constants.ROWS_PER_PAGE

	var i uint32
	fmt.Println("Num full pages: ", numFullPages)
	for i = 0; i < numFullPages; i++ {
		if pagerInstance.Pages[i] == nil {
			continue
		}
		pager.PagerFlush(pagerInstance, i, constants.PAGE_SIZE)
		pagerInstance.Pages[i] = nil
	}

	numAdditionalRows := tableInstance.NumRows % constants.ROWS_PER_PAGE
	fmt.Println("Num additional rows: ", numAdditionalRows)
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pagerInstance.Pages[pageNum] != nil {
			pager.PagerFlush(pagerInstance, pageNum, numAdditionalRows*constants.ROW_SIZE)
			pagerInstance.Pages[pageNum] = nil
		}
	}

	result := syscall.Close(pagerInstance.FileDescriptor)
	if result != nil {
		fmt.Println("Error closing db file.")
		os.Exit(1)
	}

	for i = 0; i < constants.TABLE_MAX_PAGES; i++ {
		page := pagerInstance.Pages[i]
		if page != nil {
			pagerInstance.Pages[i] = nil
		}
	}
}
