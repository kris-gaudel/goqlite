// main_test.go
package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func captureStdout(input string, f func()) string {
	oldStdin := os.Stdin
	oldStdout := os.Stdout

	defer func() {
		os.Stdin = oldStdin
		os.Stdout = oldStdout
	}()

	inRead, inWrite, _ := os.Pipe()
	inWrite.WriteString(input)
	inWrite.Close()
	os.Stdin = inRead

	outRead, outWrite, _ := os.Pipe()
	os.Stdout = outWrite

	f()

	outWrite.Close()
	outputBytes, _ := ioutil.ReadAll(outRead)
	return string(outputBytes)
}

func TestBasic(t *testing.T) {
	inputString := "insert 1 user1 person1@example.com\nselect\n.exit\n"
	expectedOutput := "db > Executed.\ndb > (1, user1, person1@example.com)\nExecuted.\ndb > "
	actualOutput := captureStdout(inputString, main)

	if actualOutput != expectedOutput {
		actualLength := len(actualOutput)
		expectedLength := len(expectedOutput)

		t.Errorf("Actual Length: %d\nExepcted Length: %d\n", actualLength, expectedLength)
		t.Errorf("Unexpected output:\nGot: %s\nExpected: %s", actualOutput, expectedOutput)
	}
}

func TestMaxLength(t *testing.T) {
	longName := strings.Repeat("a", 32)
	longEmail := strings.Repeat("a", 255)

	inputString := fmt.Sprintf("insert 1 %s %s\nselect\n.exit\n", longName, longEmail)
	expectedOutput := fmt.Sprintf("db > Executed.\ndb > (1, %s, %s)\nExecuted.\ndb > ", longName, longEmail)
	actualOutput := captureStdout(inputString, main)

	if actualOutput != expectedOutput {
		actualLength := len(actualOutput)
		expectedLength := len(expectedOutput)

		t.Errorf("Actual Length: %d\nExepcted Length: %d\n", actualLength, expectedLength)
		t.Errorf("Unexpected output:\nGot: %s\nExpected: %s", actualOutput, expectedOutput)
	}
}

func TestOverMaxLength(t *testing.T) {
	invalidLengthName := strings.Repeat("a", 33)
	invalidLengthEmail := strings.Repeat("a", 256)

	inputString := fmt.Sprintf("insert 1 %s %s\nselect\n.exit\n", invalidLengthName, invalidLengthEmail)
	expectedOutput := fmt.Sprintf("db > String is too long.\ndb > Executed.\ndb > ")
	actualOutput := captureStdout(inputString, main)

	if actualOutput != expectedOutput {
		actualLength := len(actualOutput)
		expectedLength := len(expectedOutput)

		t.Errorf("Actual Length: %d\nExepcted Length: %d\n", actualLength, expectedLength)
		t.Errorf("Unexpected output:\nGot: %s\nExpected: %s", actualOutput, expectedOutput)
	}
}

func TestNegativeId(t *testing.T) {
	inputString := "insert -1 user1 user1@test.com\n.exit\n"
	expectedOutput := "db > Syntax error. Could not parse statement.\ndb > "
	actualOutput := captureStdout(inputString, main)

	if actualOutput != expectedOutput {
		actualLength := len(actualOutput)
		expectedLength := len(expectedOutput)

		t.Errorf("Actual Length: %d\nExepcted Length: %d\n", actualLength, expectedLength)
		t.Errorf("Unexpected output:\nGot: %s\nExpected: %s", actualOutput, expectedOutput)
	}
}
