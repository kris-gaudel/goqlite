package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
)

const (
	META_COMMAND_SUCCESS              = "META_COMMAND_SUCCESS"
	META_COMMAND_FAIL                 = "META_COMMAND_FAIL"
	META_COMMAND_UNRECOGNIZED_COMMAND = "META_COMMAND_UNRECOGNIZED_COMMAND"
	META_COMMAND_EXIT                 = "META_COMMAND_EXIT"

	PREPARE_SUCCESS                = "PREPARE_SUCCESS"
	PREPARE_UNRECOGNIZED_STATEMENT = "PREPARE_UNRECOGNIZED_STATEMENT"
	PREPARE_SYNTAX_ERROR           = "PREPARE_SYNTAX_ERROR"
	// PREPARE_NEGATIVE_ID            = "PREPARE_NEGATIVE_ID" // NOTE: Not needed since Regex will not match negative numbers
	PREPARE_STRING_TOO_LONG = "PREPARE_STRING_TOO_LONG"

	STATEMENT_INSERT = "STATEMENT_INSERT"
	STATEMENT_SELECT = "STATEMENT_SELECT"
)

const (
	EXECUTE_SUCCESS        = "EXECUTE_SUCCESS"
	EXECUTE_TABLE_FULL     = "EXECUTE_TABLE_FULL"
	EXECUTE_STATEMENT_FAIL = "EXECUTE_STATEMENT_FAIL"
)

const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
)

const (
	ID_SIZE         = 4
	ID_OFFSET       = 0
	USERNAME_SIZE   = 32
	USERNAME_OFFSET = 4
	EMAIL_SIZE      = 255
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

	PAGE_SIZE       = 4096
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Row struct {
	Id       uint32
	Username [COLUMN_USERNAME_SIZE + 1]rune
	Email    [COLUMN_EMAIL_SIZE + 1]rune
}

type Statement struct {
	Type        string
	RowToInsert Row
}

type Table struct {
	NumRows uint32
	Pages   [TABLE_MAX_PAGES][]byte
}

func trimNullCharacters(input string) string {
	return strings.Trim(input, "\x00")
}

func printRow(row *Row) {
	stringifiedUsername := trimNullCharacters(string(row.Username[:]))
	stringifiedEmail := trimNullCharacters(string(row.Email[:]))
	fmt.Printf("(%d, %s, %s)\n", row.Id, stringifiedUsername, stringifiedEmail)
}

func createTable() *Table {
	table := &Table{NumRows: 0}

	for i := range table.Pages {
		table.Pages[i] = make([]byte, PAGE_SIZE)
	}

	return table
}

func serializeRow(source *Row, destination *[]byte) {
	binary.LittleEndian.PutUint32((*destination)[ID_OFFSET:ID_OFFSET+ID_SIZE], source.Id)
	copy((*destination)[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE], []byte(string(source.Username[:])))
	copy((*destination)[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE], []byte(string(source.Email[:])))
}

func deserializeRow(source []byte, destination *Row) {
	destination.Id = binary.LittleEndian.Uint32(source[ID_OFFSET : ID_OFFSET+ID_SIZE])
	copy(destination.Username[:], []rune(string(source[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE])))
	copy(destination.Email[:], []rune(string(source[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE])))
}

func rowSlot(table *Table, rowNum uint32) []byte {
	pageNum := rowNum / ROWS_PER_PAGE
	page := table.Pages[pageNum]

	if page == nil {
		page = make([]byte, PAGE_SIZE)
		table.Pages[pageNum] = page
	}

	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE

	return page[byteOffset : byteOffset+ROW_SIZE]
}

func doMetaCommand(input string, table *Table) string {
	if input == ".exit" {
		return META_COMMAND_EXIT
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepareStatement(input string, statement *Statement) string {
	if input[:6] == "insert" {
		regexPattern := `^insert (\d+) (\S+) (\S+)$`
		re := regexp.MustCompile(regexPattern)
		match := re.FindStringSubmatch(input)

		if match == nil {
			return PREPARE_SYNTAX_ERROR
		}

		id, err := strconv.Atoi(match[1])

		if err != nil {
			return PREPARE_SYNTAX_ERROR
		}

		username := match[2]
		email := match[3]

		if len(username) > COLUMN_USERNAME_SIZE || len(email) > COLUMN_EMAIL_SIZE {
			return PREPARE_STRING_TOO_LONG
		}

		statement.Type = STATEMENT_INSERT

		usernameRunes := [COLUMN_USERNAME_SIZE + 1]rune{}
		copy(usernameRunes[:], []rune(username))

		emailRunes := [COLUMN_EMAIL_SIZE + 1]rune{}
		copy(emailRunes[:], []rune(email))

		statement.RowToInsert = Row{
			Id:       uint32(id),
			Username: usernameRunes,
			Email:    emailRunes,
		}

		return PREPARE_SUCCESS
	}

	if input == "select" {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeInsert(statement *Statement, table *Table) string {
	if table.NumRows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	rowToInsert := statement.RowToInsert
	rowSlotresult := rowSlot(table, table.NumRows)
	serializeRow(&rowToInsert, &rowSlotresult)
	table.NumRows += 1

	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) string {
	var row Row
	var i uint32

	for i = 0; i < table.NumRows; i++ {
		deserializeRow(rowSlot(table, i), &row)
		printRow(&row)
	}
	return EXECUTE_SUCCESS
}

func executeStatement(statement *Statement, table *Table) string {
	switch statement.Type {
	case (STATEMENT_INSERT):
		return executeInsert(statement, table)
	case (STATEMENT_SELECT):
		return executeSelect(statement, table)
	}
	return EXECUTE_STATEMENT_FAIL
}

func main() {
	table := createTable()
	reader := bufio.NewReader(os.Stdin)
	exitFlag := false
	for {
		fmt.Print("db > ") // Prompt

		// REPL logic
		input, err := reader.ReadString('\n')
		trimmedInput := strings.TrimSpace(input)

		if err != nil {
			fmt.Print("Error reading input: ", err)
			break
		}

		if trimmedInput[0] == '.' {
			switch doMetaCommand(trimmedInput, table) {
			case (META_COMMAND_SUCCESS):
				continue
			case (META_COMMAND_UNRECOGNIZED_COMMAND):
				fmt.Println("Unrecognized command: ", trimmedInput)
				continue
			case (META_COMMAND_EXIT):
				exitFlag = true
				break
			}
		}

		if exitFlag {
			return
		}

		var statement Statement
		switch prepareStatement(trimmedInput, &statement) {
		case (PREPARE_SUCCESS):
			break
		case (PREPARE_SYNTAX_ERROR):
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case (PREPARE_UNRECOGNIZED_STATEMENT):
			fmt.Println("Unrecognized keyword at start of: ", trimmedInput)
			continue
		case (PREPARE_STRING_TOO_LONG):
			fmt.Println("String is too long.")
			continue
			// case (PREPARE_NEGATIVE_ID):
			// 	fmt.Println("ID must be positive.")
			// 	continue
		}

		switch executeStatement(&statement, table) {
		case (EXECUTE_SUCCESS):
			fmt.Println("Executed.")
			break
		case (EXECUTE_TABLE_FULL):
			fmt.Println("Error: Table full.")
			break
		default:
			fmt.Println("Default")
		}
	}
}
