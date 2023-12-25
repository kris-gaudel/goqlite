package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const (
	META_COMMAND_SUCCESS           = "META_COMMAND_SUCCESS"
	META_COMMAND_FAIL              = "META_COMMAND_FAIL"
	PREPARE_SUCCESS                = "PREPARE_SUCCESS"
	PREPARE_UNRECOGNIZED_STATEMENT = "PREPARE_UNRECOGNIZED_STATEMENT"
	STATEMENT_INSERT               = "STATEMENT_INSERT"
	STATEMENT_SELECT               = "STATEMENT_SELECT"
)

type Statement struct {
	Type string
}

func doMetaCommand(input string) string {
	if input == ".exit" {
		os.Exit(0)
	}
	return META_COMMAND_FAIL
}

func prepareStatement(input string, statement *Statement) string {
	if input[:6] == "insert" {
		statement.Type = STATEMENT_INSERT
		return PREPARE_SUCCESS
	}

	if input == "select" {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}
	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeStatement(statement *Statement) {
	switch statement.Type {
	case (STATEMENT_INSERT):
		fmt.Println("Handle INSERT")
		break
	case (STATEMENT_SELECT):
		fmt.Println("Handle SELECT")
		break
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
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
			switch doMetaCommand(trimmedInput) {
			case (META_COMMAND_SUCCESS):
				continue
			case (META_COMMAND_FAIL):
				fmt.Println("Unrecognized command: ", trimmedInput)
				continue
			}
		}

		var statement Statement
		switch prepareStatement(trimmedInput, &statement) {
		case (PREPARE_SUCCESS):
			break
		case (PREPARE_UNRECOGNIZED_STATEMENT):
			fmt.Println("Unrecognized keyword at start of: ", trimmedInput)
			continue
		}

		executeStatement(&statement)
		fmt.Println("Executed")
	}
}
