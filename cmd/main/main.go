package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/kris-gaudel/goqlite/constants"
	parsecommand "github.com/kris-gaudel/goqlite/parseCommand"
	"github.com/kris-gaudel/goqlite/table"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: ./goqlite DB_FILE_NAME")
		os.Exit(1)
	}

	fileName := os.Args[1]
	table := table.DBOpen(fileName)

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
			switch parsecommand.DoMetaCommand(trimmedInput, table) {
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

		var statement parsecommand.Statement
		switch parsecommand.PrepareStatement(trimmedInput, &statement) {
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

		switch parsecommand.ExecuteStatement(&statement, table) {
		case (constants.EXECUTE_SUCCESS):
			fmt.Println("Executed.")
			break
		case (constants.EXECUTE_TABLE_FULL):
			fmt.Println("Error: Table full.")
			break
		default:
			fmt.Println("Default")
		}
	}
}
