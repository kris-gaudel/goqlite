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
	"unsafe"

	"github.com/kris-gaudel/goqlite/constants"
)

// Structs
type Row struct {
	Id       uint32
	Username [constants.COLUMN_USERNAME_SIZE + 1]rune
	Email    [constants.COLUMN_EMAIL_SIZE + 1]rune
}

type Statement struct {
	Type        string
	RowToInsert Row
}

type Pager struct {
	FileDescriptor int
	FileLength     uint32
	NumPages       uint32
	Pages          [constants.TABLE_MAX_PAGES][]byte
}

type Table struct {
	RootPageNum uint32
	Pager       *Pager
}

type Cursor struct {
	Table      *Table
	PageNum    uint32
	CellNum    uint32
	EndOfTable bool
}

// Utility Code

func PrintRow(row *Row) {
	stringifiedUsername := trimNullCharacters(string(row.Username[:]))
	stringifiedEmail := trimNullCharacters(string(row.Email[:]))
	fmt.Printf("(%d, %s, %s)\n", row.Id, stringifiedUsername, stringifiedEmail)
}

func PrintConstants() {
	fmt.Printf("ROW_SIZE: %d\n", constants.ROW_SIZE)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", constants.COMMON_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", constants.LEAF_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", constants.LEAF_NODE_CELL_SIZE)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", constants.LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", constants.LEAF_NODE_MAX_CELLS)
}

func PrintLeafNode(nodeInstance []byte) {
	numCells := *LeafNodeNumCells(nodeInstance)
	fmt.Printf("leaf (size %d)\n", numCells)
	for i := uint32(0); i < numCells; i++ {
		key := *LeafNodeKey(nodeInstance, i)
		fmt.Printf(" - %d : %d\n", i, key)
	}
}

func uint32ToBytes(value uint32) []byte {
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, value)
	return bytes
}

// Leaf Node Code

func LeafNodeNumCells(nodeInstance []byte) *uint32 {
	return (*uint32)(unsafe.Pointer(&nodeInstance[constants.LEAF_NODE_NUM_CELLS_OFFSET]))
}

func LeafNodeCell(nodeInstance []byte, cellNum uint32) []byte {
	offset := uint32(constants.LEAF_NODE_HEADER_SIZE) + cellNum*uint32(constants.LEAF_NODE_CELL_SIZE)
	return nodeInstance[offset : offset+uint32(constants.LEAF_NODE_CELL_SIZE)]
}

func LeafNodeKey(nodeInstance []byte, cellNum uint32) *uint32 {
	// value := binary.LittleEndian.Uint32(LeafNodeCell(nodeInstance, cellNum))
	// fmt.Println("LeafNodeKey - Key is: ", value)
	// return &value
	return (*uint32)(unsafe.Pointer(&nodeInstance[constants.LEAF_NODE_HEADER_SIZE+uintptr(cellNum)*constants.LEAF_NODE_CELL_SIZE]))
}

func LeafNodeValue(nodeInstance []byte, cellNum uint32) []byte {
	return LeafNodeCell(nodeInstance, cellNum)[constants.LEAF_NODE_KEY_OFFSET : constants.LEAF_NODE_KEY_OFFSET+constants.LEAF_NODE_KEY_SIZE]
}

func InitializeLeafNode(nodeInstance []byte) {
	SetNodeType(nodeInstance, constants.NODE_LEAF)
	*LeafNodeNumCells(nodeInstance) = 0
}

func LeafNodeInsert(cursorInstance *Cursor, key uint32, value *Row) {
	nodeInstance := GetPage(cursorInstance.Table.Pager, cursorInstance.PageNum)
	numCells := *LeafNodeNumCells(nodeInstance)

	if numCells >= uint32(constants.LEAF_NODE_MAX_CELLS) {
		fmt.Println("Need to implement splitting a leaf node.")
		os.Exit(1)
	}

	if cursorInstance.CellNum < numCells {
		for i := numCells; i > cursorInstance.CellNum; i-- {
			copy(LeafNodeCell(nodeInstance, i), LeafNodeCell(nodeInstance, i-1))
		}
	}

	*LeafNodeNumCells(nodeInstance) += 1
	*LeafNodeKey(nodeInstance, cursorInstance.CellNum) = key
	SerializeRow(value, LeafNodeValue(nodeInstance, cursorInstance.CellNum))
}

func LeafNodeFind(tableInstance *Table, pageNum uint32, key uint32) *Cursor {
	node := GetPage(tableInstance.Pager, pageNum)
	numCells := *LeafNodeNumCells(node)

	cursorInstance := &Cursor{Table: tableInstance, PageNum: pageNum}

	// Binary search
	minIndex := uint32(0)
	onePastMaxIndex := numCells
	for onePastMaxIndex != minIndex {
		index := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := *LeafNodeKey(node, index)
		if key == keyAtIndex {
			cursorInstance.CellNum = index
			return cursorInstance
		}
		if key < keyAtIndex {
			onePastMaxIndex = index
		} else {
			minIndex = index + 1
		}
	}
	cursorInstance.CellNum = minIndex
	return cursorInstance
}

func GetNodeType(nodeInstance []byte) constants.NodeType {
	value := *(*uint8)(unsafe.Pointer(&nodeInstance[constants.NODE_TYPE_OFFSET]))
	return constants.NodeType(value)
}

func SetNodeType(nodeInstance []byte, nodeType constants.NodeType) {
	value := uint8(nodeType)
	*(*uint8)(unsafe.Pointer(&nodeInstance[constants.NODE_TYPE_OFFSET])) = value
}

// Cursor Code

func TableStart(tableInstance *Table) *Cursor {
	cursor := &Cursor{Table: tableInstance, PageNum: tableInstance.RootPageNum, CellNum: 0}

	rootNode := GetPage(tableInstance.Pager, tableInstance.RootPageNum)
	numCells := *LeafNodeNumCells(rootNode)
	cursor.EndOfTable = (numCells == 0)
	return cursor
}

func TableFind(tableInstance *Table, key uint32) *Cursor {
	rootPageNum := tableInstance.RootPageNum
	rootNode := GetPage(tableInstance.Pager, rootPageNum)

	if (GetNodeType(rootNode)) != constants.NODE_LEAF {
		fmt.Println("Need to implement searching an internal node.")
		os.Exit(1)
	}
	return LeafNodeFind(tableInstance, rootPageNum, key)
}

func CursorValue(cursor *Cursor) []byte {
	pageNum := cursor.PageNum
	page := GetPage(cursor.Table.Pager, pageNum)

	return LeafNodeValue(page, cursor.CellNum)
}

func CursorAdvance(cursor *Cursor) {
	pageNum := cursor.PageNum
	nodeInstance := GetPage(cursor.Table.Pager, pageNum)

	cursor.CellNum += 1
	if cursor.CellNum >= *LeafNodeNumCells(nodeInstance) {
		cursor.EndOfTable = true
	}
}

// Pager Code

func PagerOpen(fileName string) *Pager {
	fd, err := syscall.Open(fileName, constants.O_RDWR|constants.O_CREAT, constants.S_IWUSR|constants.S_IRUSR)
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
		NumPages:       uint32(fileLength / constants.PAGE_SIZE),
	}

	if fileLength%constants.PAGE_SIZE != 0 {
		fmt.Println("Db file is not a whole number of pages. Corrupt file.")
		os.Exit(1)
	}

	for i := 0; i < constants.TABLE_MAX_PAGES; i++ {
		pager.Pages[i] = nil
	}

	return pager
}

func PagerFlush(pager *Pager, pageNum uint32) {
	if pager.Pages[pageNum] == nil {
		fmt.Println("Tried to flush null page.")
		os.Exit(1)
	}

	_, err := syscall.Seek(pager.FileDescriptor, int64(pageNum*constants.PAGE_SIZE), os.SEEK_SET)
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

func GetPage(pagerInstance *Pager, pageNum uint32) []byte {
	if pageNum > constants.TABLE_MAX_PAGES {
		fmt.Println("Tried to fetch page number out of bounds.")
		os.Exit(1)
	}

	if pagerInstance.Pages[pageNum] == nil {
		page := make([]byte, constants.PAGE_SIZE)
		numPages := pagerInstance.FileLength / constants.PAGE_SIZE

		if pagerInstance.FileLength%constants.PAGE_SIZE != 0 {
			numPages += 1
		}

		if pageNum <= numPages {
			_, errSeek := syscall.Seek(pagerInstance.FileDescriptor, int64(pageNum*constants.PAGE_SIZE), os.SEEK_SET)
			_, errRead := syscall.Read(pagerInstance.FileDescriptor, page)
			if errSeek != nil {
				fmt.Println("Error seeking file: ", errSeek)
				os.Exit(1)
			}
			if errRead != nil {
				fmt.Println("Error reading file: ", errRead)
				os.Exit(1)
			}
		}
		pagerInstance.Pages[pageNum] = page
	}

	if pageNum >= pagerInstance.NumPages {
		pagerInstance.NumPages = pageNum + 1
	}

	return pagerInstance.Pages[pageNum]
}

// Table Code

func trimNullCharacters(input string) string {
	return strings.Trim(input, "\x00")
}

func SerializeRow(source *Row, destination []byte) {
	binary.LittleEndian.PutUint32((destination)[constants.ID_OFFSET:constants.ID_OFFSET+constants.ID_SIZE], source.Id)
	copy((destination)[constants.USERNAME_OFFSET:constants.USERNAME_OFFSET+constants.USERNAME_SIZE], []byte(trimNullCharacters(string(source.Username[:constants.USERNAME_SIZE]))))
	copy((destination)[constants.EMAIL_OFFSET:constants.EMAIL_OFFSET+constants.EMAIL_SIZE], []byte(trimNullCharacters(string(source.Email[:constants.EMAIL_SIZE]))))
}

func DeserializeRow(source []byte, destination *Row) {
	destination.Id = binary.LittleEndian.Uint32(source[constants.ID_OFFSET : constants.ID_OFFSET+constants.ID_SIZE])
	copy(destination.Username[:], []rune(trimNullCharacters(string(source[constants.USERNAME_OFFSET:constants.USERNAME_OFFSET+constants.USERNAME_OFFSET]))))
	copy(destination.Email[:], []rune(trimNullCharacters(string(source[constants.EMAIL_OFFSET:constants.EMAIL_OFFSET+constants.EMAIL_SIZE]))))
}

func DBOpen(fileName string) *Table {
	pagerInstance := PagerOpen(fileName)
	table := &Table{RootPageNum: 0, Pager: pagerInstance}
	if pagerInstance.FileLength == 0 {
		rootNode := GetPage(pagerInstance, 0)
		InitializeLeafNode(rootNode)
	}
	return table
}

func DBClose(tableInstance *Table) {
	pagerInstance := tableInstance.Pager

	var i uint32
	for i = 0; i < pagerInstance.NumPages; i++ {
		if pagerInstance.Pages[i] == nil {
			continue
		}
		PagerFlush(pagerInstance, i)
		pagerInstance.Pages[i] = nil
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

// Parse Command Code

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

		statement.RowToInsert = Row{
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

func DoMetaCommand(input string, tableInstance *Table) string {
	if input == ".exit" {
		DBClose(tableInstance)
		return constants.META_COMMAND_EXIT
	} else if input == ".constants" {
		fmt.Println("Constants:")
		PrintConstants()
		return constants.META_COMMAND_SUCCESS
	} else if input == ".btree" {
		fmt.Println("Tree:")
		PrintLeafNode(GetPage(tableInstance.Pager, 0))
		return constants.META_COMMAND_SUCCESS
	}
	return constants.META_COMMAND_UNRECOGNIZED_COMMAND
}

func ExecuteInsert(statement *Statement, tableInstance *Table) string {
	node := GetPage(tableInstance.Pager, tableInstance.RootPageNum)
	numCells := *LeafNodeNumCells(node)
	if numCells >= uint32(constants.LEAF_NODE_MAX_CELLS) {
		return constants.EXECUTE_TABLE_FULL
	}

	rowToInsert := statement.RowToInsert
	keyToInsert := rowToInsert.Id
	cursorInstance := TableFind(tableInstance, keyToInsert)

	if cursorInstance.CellNum < numCells {
		keyAtIndex := *LeafNodeKey(node, cursorInstance.CellNum)
		if keyAtIndex == keyToInsert {
			return constants.EXECUTE_DUPLICATE_KEY
		}
	}

	LeafNodeInsert(cursorInstance, rowToInsert.Id, &rowToInsert)

	return constants.EXECUTE_SUCCESS
}

func ExecuteSelect(statement *Statement, tableInstance *Table) string {
	cursorInstance := TableStart(tableInstance)
	var row Row

	for {
		if cursorInstance.EndOfTable {
			break
		}
		DeserializeRow(CursorValue(cursorInstance), &row)
		if row.Id != 0 {
			// Note: Figure out why pager considers "junk" entries with ID = 0 to be valid,
			// can simply filter them out and functions as normal
			PrintRow(&row)
		}
		CursorAdvance(cursorInstance)
	}

	return constants.EXECUTE_SUCCESS
}

func ExecuteStatement(statement *Statement, tableInstance *Table) string {
	switch statement.Type {
	case (constants.STATEMENT_INSERT):
		return ExecuteInsert(statement, tableInstance)
	case (constants.STATEMENT_SELECT):
		return ExecuteSelect(statement, tableInstance)
	}
	return constants.EXECUTE_STATEMENT_FAIL
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./goqlite DB_FILE_NAME")
		os.Exit(1)
	}

	fileName := os.Args[1]
	table := DBOpen(fileName)

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
			switch DoMetaCommand(trimmedInput, table) {
			case (constants.META_COMMAND_SUCCESS):
				continue
			case (constants.META_COMMAND_UNRECOGNIZED_COMMAND):
				fmt.Println("Unrecognized command: ", trimmedInput)
				continue
			case (constants.META_COMMAND_EXIT):
				exitFlag = true
				break
			}
		}

		if exitFlag {
			return
		}

		var statement Statement
		switch PrepareStatement(trimmedInput, &statement) {
		case (constants.PREPARE_SUCCESS):
			break
		case (constants.PREPARE_SYNTAX_ERROR):
			fmt.Println("Syntax error. Could not parse statement.")
			continue
		case (constants.PREPARE_UNRECOGNIZED_STATEMENT):
			fmt.Println("Unrecognized keyword at start of: ", trimmedInput)
			continue
		case (constants.PREPARE_STRING_TOO_LONG):
			fmt.Println("String is too long.")
			continue
		// case (PREPARE_NEGATIVE_ID):
		// 	fmt.Println("ID must be positive.")
		// 	continue
		case (constants.PREPARE_NON_POSITIVE_ID):
			fmt.Println("ID must be positive")
			continue
		}

		switch ExecuteStatement(&statement, table) {
		case (constants.EXECUTE_SUCCESS):
			fmt.Println("Executed.")
			break
		case (constants.EXECUTE_TABLE_FULL):
			fmt.Println("Error: Table full.")
			break
		case (constants.EXECUTE_DUPLICATE_KEY):
			fmt.Println("Error: Duplicate key.")
			break
		default:
			fmt.Println("Default")
		}
	}
}
