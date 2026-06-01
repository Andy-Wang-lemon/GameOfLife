package gol

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"uk.ac.bris.cs/gameoflife/util"
)

// a small enum for what the IO worker should do
type ioCommand uint8

const (
	ioOutput ioCommand = iota
	ioInput
	ioCheckIdle
)

// this struct holds shared state between distributor and IO goroutine
// kind of like a mailbox where distributor drops requests
type ioState struct {
	params Params

	lock *sync.Mutex
	note *sync.Cond

	// stuff that distributor fills in when asking for work
	hasRequest bool
	activeCmd  ioCommand
	targetFile string
	worldView  [][]byte // the image buffer to read/write

	// flags telling whether the work is done and whether IO worker is resting
	finished bool
	isIdle   bool
}

// create a new shared IO state
// both distributor and IO goroutine will use the same pointer
func newIOState(p Params) *ioState {
	m := &sync.Mutex{}
	st := &ioState{
		params: p,
		lock:   m,
		isIdle: true,
	}
	st.note = sync.NewCond(st.lock)
	return st
}

// distributor asks IO worker to read an image file
// this blocks until the read is done
func (io *ioState) ioReadImage(fileName string, worldBuf [][]byte) {
	io.lock.Lock()

	// drop the request into the shared state
	io.hasRequest = true
	io.finished = false
	io.isIdle = false
	io.activeCmd = ioInput
	io.targetFile = fileName
	io.worldView = worldBuf

	// poke IO goroutine to wake up
	io.note.Signal()

	// wait until IO worker says it's done
	for !io.finished {
		io.note.Wait()
	}
	io.lock.Unlock()
}

// distributor asks IO worker to write an image file
// also blocks until it's done
func (io *ioState) ioWriteImage(fileName string, worldBuf [][]byte) {
	io.lock.Lock()

	io.hasRequest = true
	io.finished = false
	io.isIdle = false
	io.activeCmd = ioOutput
	io.targetFile = fileName
	io.worldView = worldBuf

	io.note.Signal()

	for !io.finished {
		io.note.Wait()
	}
	io.lock.Unlock()
}

// helper if distributor wants to wait until IO worker is idle
func (io *ioState) ioWaitIdle() {
	io.lock.Lock()
	for !io.isIdle {
		io.note.Wait()
	}
	io.lock.Unlock()
}

// this is the loop run by the IO goroutine
// it just waits for new requests and does them one by one
func startIo(io *ioState) {
	for {
		io.lock.Lock()

		// wait here until someone gives us a job
		for !io.hasRequest {
			io.note.Wait()
		}

		// copy request so we can release the lock while working
		cmd := io.activeCmd
		fileName := io.targetFile
		worldBuf := io.worldView

		io.hasRequest = false
		io.lock.Unlock()

		// do the actual work outside the lock
		switch cmd {
		case ioInput:
			io.readPgmImage(fileName, worldBuf)
		case ioOutput:
			io.writePgmImage(fileName, worldBuf)
		case ioCheckIdle:
			// this one is basically unused now
		}

		// mark job as done and mark idle again
		io.lock.Lock()
		io.finished = true
		io.isIdle = true
		io.note.Broadcast()
		io.lock.Unlock()
	}
}

// write a pgm image
// this takes the 2D world array and dumps it into a file
func (io *ioState) writePgmImage(fileName string, world [][]byte) {
	_ = os.Mkdir("out", os.ModePerm)

	file, ioError := os.Create("out/" + fileName + ".pgm")
	util.Check(ioError)
	defer file.Close()

	// write header stuff
	file.WriteString("P5\n")
	file.WriteString(strconv.Itoa(io.params.ImageWidth))
	file.WriteString(" ")
	file.WriteString(strconv.Itoa(io.params.ImageHeight))
	file.WriteString("\n")
	file.WriteString("255\n")

	// write pixel data
	for y := 0; y < io.params.ImageHeight; y++ {
		for x := 0; x < io.params.ImageWidth; x++ {
			_, ioError = file.Write([]byte{world[y][x]})
			util.Check(ioError)
		}
	}

	ioError = file.Sync()
	util.Check(ioError)

	log.Printf("[IO] File %v.pgm output done", fileName)
}

// read a pgm image
// loads a file and fills the given 2D array with pixel bytes
func (io *ioState) readPgmImage(fileName string, world [][]byte) {
	data, ioError := os.ReadFile("images/" + fileName + ".pgm")
	util.Check(ioError)

	fields := strings.Fields(string(data))

	if len(fields) < 5 {
		panic(fmt.Sprintf("[IO] %v %v is not a valid pgm file", util.Red("ERROR"), fileName))
	}

	if fields[0] != "P5" {
		panic(fmt.Sprintf("[IO] %v %v is not a pgm file", util.Red("ERROR"), fileName))
	}

	width, _ := strconv.Atoi(fields[1])
	if width != io.params.ImageWidth {
		panic(fmt.Sprintf("[IO] %v Incorrect pgm width", util.Red("ERROR")))
	}

	height, _ := strconv.Atoi(fields[2])
	if height != io.params.ImageHeight {
		panic(fmt.Sprintf("[IO] %v Incorrect pgm height", util.Red("ERROR")))
	}

	maxval, _ := strconv.Atoi(fields[3])
	if maxval != 255 {
		panic(fmt.Sprintf("[IO] %v Incorrect pgm maxval/bit depth", util.Red("ERROR")))
	}

	image := []byte(fields[4])
	expected := io.params.ImageWidth * io.params.ImageHeight
	if len(image) < expected {
		panic(fmt.Sprintf("[IO] %v pgm data too short", util.Red("ERROR")))
	}

	// fill world[y][x] by walking through the raw byte slice
	idx := 0
	for y := 0; y < io.params.ImageHeight; y++ {
		for x := 0; x < io.params.ImageWidth; x++ {
			world[y][x] = image[idx]
			idx++
		}
	}

	log.Printf("[IO] File %v.pgm input done", fileName)
}
