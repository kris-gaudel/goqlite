package parsecommand

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/kris-gaudel/goqlite/constants"
	"github.com/kris-gaudel/goqlite/cursor"
	"github.com/kris-gaudel/goqlite/table"
)

type Statement struct {
	Type        string
	RowToInsert table.Row
}

func PrepareStatement(input string, statement *Statement) string {
	if len(input) >= 6 && input[:6] == "insert" {
		regexPattern := `^insert (\d+) (\S+) (\S+)$`
		re := regexp.MustCompile(regexPattern)
		match := re.FindStringSubmatch(input)

		if match == nil {
			return constants.PREPARE_SYNTAX_ERROR
		}

		id, err := strconv.Atoi(match[1])

		if id <= 0 {
			return constants.PREPARE_NON_POSITIVE_ID
		}

		if err != nil {
			return constants.PREPARE_SYNTAX_ERROR
		}

		username := match[2]
		email := match[3]

		if len(username) > constants.COLUMN_USERNAME_SIZE || len(email) > constants.COLUMN_EMAIL_SIZE {
			return constants.PREPARE_STRING_TOO_LONG
		}

		statement.Type = constants.STATEMENT_INSERT

		usernameRunes := [constants.COLUMN_USERNAME_SIZE + 1]rune{}
		copy(usernameRunes[:], []rune(username))

		emailRunes := [constants.COLUMN_EMAIL_SIZE + 1]rune{}
		copy(emailRunes[:], []rune(email))

		statement.RowToInsert = table.Row{
			Id:       uint32(id),
			Username: usernameRunes,
			Email:    emailRunes,
		}

		return constants.PREPARE_SUCCESS
	}

	if input == "select" {
		statement.Type = constants.STATEMENT_SELECT
		return constants.PREPARE_SUCCESS
	}
	return constants.PREPARE_UNRECOGNIZED_STATEMENT
}

func DoMetaCommand(input string, tableInstance *table.Table) string {
	if input == ".exit" {
		table.DBClose(tableInstance)
		return constants.META_COMMAND_EXIT
	}
	return constants.META_COMMAND_UNRECOGNIZED_COMMAND
}

func ExecuteInsert(statement *Statement, tableInstance *table.Table) string {
	fmt.Println("Table rows: ", tableInstance.NumRows)
	if tableInstance.NumRows >= constants.TABLE_MAX_ROWS {
		return constants.EXECUTE_TABLE_FULL
	}

	rowToInsert := statement.RowToInsert
	cursorInstance := cursor.TableEnd(tableInstance)

	rowSlotresult := cursor.CursorValue(cursorInstance)

	table.SerializeRow(&rowToInsert, &rowSlotresult)
	tableInstance.NumRows += 1

	return constants.EXECUTE_SUCCESS
}

func ExecuteSelect(statement *Statement, tableInstance *table.Table) string {
	cursorInstance := cursor.TableStart(tableInstance)
	var row table.Row

	for {
		if cursorInstance.EndOfTable {
			break
		}
		table.DeserializeRow(cursor.CursorValue(cursorInstance), &row)
		if row.Id != 0 {
			// Note: Figure out why pager considers "junk" entries with ID = 0 to be valid,
			// can simply filter them out and functions as normal
			table.PrintRow(&row)
		}
		cursor.CursorAdvance(cursorInstance)
	}

	return constants.EXECUTE_SUCCESS
}

func ExecuteStatement(statement *Statement, tableInstance *table.Table) string {
	switch statement.Type {
	case (constants.STATEMENT_INSERT):
		return ExecuteInsert(statement, tableInstance)
	case (constants.STATEMENT_SELECT):
		return ExecuteSelect(statement, tableInstance)
	}
	return constants.EXECUTE_STATEMENT_FAIL
}
