package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"syscall"
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
	PREPARE_NON_POSITIVE_ID = "PREPARE_NON_POSITIVE_ID"

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
	ID_SIZE       = 4
	USERNAME_SIZE = 32
	EMAIL_SIZE    = 255

	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE

	PAGE_SIZE       = 4096
	TABLE_MAX_PAGES = 100
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

const (
	DEFAULT_FILE_MODE = os.FileMode(0644)
)

const (
	O_RDWR  = syscall.O_RDWR
	O_CREAT = syscall.O_CREAT
	S_IWUSR = syscall.S_IWUSR
	S_IRUSR = syscall.S_IRUSR
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

type Pager struct {
	FileDescriptor int
	FileLength     uint32
	Pages          [TABLE_MAX_PAGES][]byte
}

type Table struct {
	NumRows uint32
	Pager   *Pager
}

func trimNullCharacters(input string) string {
	return strings.Trim(input, "\x00")
}

// func trimNullCharacters(input []rune) []rune {
// 	var result []rune

// 	for _, r := range input {
// 		if r != 0 {
// 			result = append(result, r)
// 		}
// 	}

// 	return result
// }

func printRow(row *Row) {
	stringifiedUsername := trimNullCharacters(string(row.Username[:]))
	stringifiedEmail := trimNullCharacters(string(row.Email[:]))
	fmt.Printf("(%d, %s, %s)\n", row.Id, stringifiedUsername, stringifiedEmail)
}

// func createTable() *Table {
// 	table := &Table{NumRows: 0}

// 	for i := range table.Pages {
// 		table.Pages[i] = make([]byte, PAGE_SIZE)
// 	}

// 	return table
// }

func pagerOpen(fileName string) *Pager {
	fd, err := syscall.Open(fileName, O_RDWR|O_CREAT, S_IWUSR|S_IRUSR)
	fmt.Println("File descriptor: ", fd)
	if err != nil {
		fmt.Println("Unable to open file")
		os.Exit(1)
	}

	fileLength, err := syscall.Seek(fd, 0, os.SEEK_END)
	if err != nil {
		fmt.Println("Error getting file length")
		os.Exit(1)
	}

	// fileLength, err := os.Stat(fileName)
	// if err != nil {
	// 	fmt.Println("Error getting file length")
	// 	os.Exit(1)
	// }

	pager := &Pager{
		FileDescriptor: fd,
		FileLength:     uint32(fileLength),
	}

	for i := 0; i < TABLE_MAX_PAGES; i++ {
		pager.Pages[i] = nil
	}

	return pager
}

func dbOpen(fileName string) *Table {
	pager := pagerOpen(fileName)
	fmt.Println("File length: ", pager.FileLength)
	numRows := pager.FileLength / ROW_SIZE
	fmt.Println("Num rows: ", numRows)

	table := &Table{NumRows: numRows, Pager: pager}
	return table
}

func pagerFlush(pager *Pager, pageNum uint32, size uint32) {
	if pager.Pages[pageNum] == nil {
		fmt.Println("Tried to flush null page.")
		os.Exit(1)
	}

	_, err := syscall.Seek(pager.FileDescriptor, int64(pageNum*PAGE_SIZE), os.SEEK_SET)
	if err != nil {
		fmt.Println("Error seeking: ", err)
		os.Exit(1)
	}

	bytesWritten, err := syscall.Write(pager.FileDescriptor, pager.Pages[pageNum])
	if bytesWritten == 0 || err != nil {
		fmt.Println("Error writing: ", err)
		os.Exit(1)
	}
}

func dbClose(table *Table) {
	pager := table.Pager
	numFullPages := table.NumRows / ROWS_PER_PAGE

	var i uint32
	fmt.Println("Num full pages: ", numFullPages)
	for i = 0; i < numFullPages; i++ {
		if pager.Pages[i] == nil {
			continue
		}
		pagerFlush(pager, i, PAGE_SIZE)
		pager.Pages[i] = nil
	}

	numAdditionalRows := table.NumRows % ROWS_PER_PAGE
	fmt.Println("Num additional rows: ", numAdditionalRows)
	if numAdditionalRows > 0 {
		pageNum := numFullPages
		if pager.Pages[pageNum] != nil {
			pagerFlush(pager, pageNum, numAdditionalRows*ROW_SIZE)
			pager.Pages[pageNum] = nil
		}
	}

	result := syscall.Close(pager.FileDescriptor)
	if result != nil {
		fmt.Println("Error closing db file.")
		os.Exit(1)
	}

	for i = 0; i < TABLE_MAX_PAGES; i++ {
		page := pager.Pages[i]
		if page != nil {
			pager.Pages[i] = nil
		}
	}
}

func serializeRow(source *Row, destination *[]byte) {
	binary.LittleEndian.PutUint32((*destination)[ID_OFFSET:ID_OFFSET+ID_SIZE], source.Id)
	copy((*destination)[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE], []byte(trimNullCharacters(string(source.Username[:USERNAME_SIZE]))))
	copy((*destination)[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE], []byte(trimNullCharacters(string(source.Email[:EMAIL_SIZE]))))
}

func deserializeRow(source []byte, destination *Row) {
	destination.Id = binary.LittleEndian.Uint32(source[ID_OFFSET : ID_OFFSET+ID_SIZE])
	copy(destination.Username[:], []rune(trimNullCharacters(string(source[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE]))))
	copy(destination.Email[:], []rune(trimNullCharacters(string(source[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE]))))
}

func getPage(pager *Pager, pageNum uint32) []byte {
	if pageNum > TABLE_MAX_PAGES {
		fmt.Println("Tried to fetch page number out of bounds.")
		os.Exit(1)
	}

	if pager.Pages[pageNum] == nil {
		page := make([]byte, PAGE_SIZE)
		numPages := pager.FileLength / PAGE_SIZE

		if pager.FileLength%PAGE_SIZE != 0 {
			numPages += 1
		}

		if pageNum <= numPages {
			_, errSeek := syscall.Seek(pager.FileDescriptor, int64(pageNum*PAGE_SIZE), os.SEEK_SET)
			_, errRead := syscall.Read(pager.FileDescriptor, page)
			if errSeek != nil {
				fmt.Println("Error seeking file: ", errSeek)
				os.Exit(1)
			}
			if errRead != nil {
				fmt.Println("Error reading file: ", errRead)
				os.Exit(1)
			}
		}
		pager.Pages[pageNum] = page
	}

	return pager.Pages[pageNum]
}

func rowSlot(table *Table, rowNum uint32) []byte {
	pageNum := rowNum / ROWS_PER_PAGE

	page := getPage(table.Pager, pageNum)
	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE

	return page[byteOffset:]
}

func doMetaCommand(input string, table *Table) string {
	if input == ".exit" {
		dbClose(table)
		return META_COMMAND_EXIT
	}
	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepareStatement(input string, statement *Statement) string {
	if len(input) >= 6 && input[:6] == "insert" {
		regexPattern := `^insert (\d+) (\S+) (\S+)$`
		re := regexp.MustCompile(regexPattern)
		match := re.FindStringSubmatch(input)

		if match == nil {
			return PREPARE_SYNTAX_ERROR
		}

		id, err := strconv.Atoi(match[1])

		if id <= 0 {
			return PREPARE_NON_POSITIVE_ID
		}

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
	fmt.Println("Table rows: ", table.NumRows)
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
		if row.Id != 0 {
			// Note: Figure out why pager considers "junk" entries with ID = 0 to be valid,
			// can simply filter them out and functions as normal
			printRow(&row)
		}
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
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./goqlite DB_FILE_NAME")
		os.Exit(1)
	}

	fileName := os.Args[1]
	table := dbOpen(fileName)

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
		case (PREPARE_NON_POSITIVE_ID):
			fmt.Println("ID must be positive")
			continue
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
