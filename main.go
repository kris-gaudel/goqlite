package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("db > ") // Prompt

		// REPL logic
		input, err := reader.ReadString('\n')
		if err != nil {
			fmt.Print("Error reading input: ", err)
			break
		}

		if input == ".exit\n" {
			break
		} else {
			fmt.Print("Unrecognized command: ", input)
		}
	}
}
