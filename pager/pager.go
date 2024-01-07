package pager

import (
	"fmt"
	"os"
	"syscall"

	"github.com/kris-gaudel/goqlite/constants"
)

type Pager struct {
	FileDescriptor int
	FileLength     uint32
	Pages          [constants.TABLE_MAX_PAGES][]byte
}

func PagerOpen(fileName string) *Pager {
	fd, err := syscall.Open(fileName, constants.O_RDWR|constants.O_CREAT, constants.S_IWUSR|constants.S_IRUSR)
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

	for i := 0; i < constants.TABLE_MAX_PAGES; i++ {
		pager.Pages[i] = nil
	}

	return pager
}

func PagerFlush(pager *Pager, pageNum uint32, size uint32) {
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

	return pagerInstance.Pages[pageNum]
}
