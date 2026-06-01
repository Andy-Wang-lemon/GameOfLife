package gol

import (
	"fmt"
	"sync"
	"time"

	"uk.ac.bris.cs/gameoflife/util"
)

// eventHub is a tiny event queue shared by multiple goroutines
// distributor drops events in here, some other goroutine can pull them out later
type eventHub struct {
	lock     sync.Mutex
	wakeUp   *sync.Cond
	pending  []Event
	isClosed bool
}

// make a fresh hub, nothing fancy here
func newEventHub() *eventHub {
	hub := &eventHub{}
	hub.wakeUp = sync.NewCond(&hub.lock)
	return hub
}

// postEvent just adds an event into the queue and pokes whoever is waiting
func (hub *eventHub) postEvent(e Event) {
	hub.lock.Lock()
	defer hub.lock.Unlock()

	// if we are closed, pretend nothing happened
	if hub.isClosed {
		return
	}
	hub.pending = append(hub.pending, e)
	hub.wakeUp.Signal()
}

// shutDown marks hub as closed and wakes up any sleepers
// so nobody gets stuck forever waiting on the cond
func (hub *eventHub) shutDown() {
	hub.lock.Lock()
	hub.isClosed = true
	hub.wakeUp.Broadcast()
	hub.lock.Unlock()
}

// keyBuffer is just a simple queue for keyboard input
// another goroutine can push keys in, distributor grabs them out
type keyBuffer struct {
	lock sync.Mutex
	keys []rune
}

// make a new empty key buffer
func newKeyBuffer() *keyBuffer {
	return &keyBuffer{}
}

// pushKey stores one key press into the buffer
func (kb *keyBuffer) pushKey(k rune) {
	kb.lock.Lock()
	kb.keys = append(kb.keys, k)
	kb.lock.Unlock()
}

// grabKey tries to take one key out of the buffer
// returns ok=false if there is nothing right now (non-blocking style)
func (kb *keyBuffer) grabKey() (rune, bool) {
	kb.lock.Lock()
	defer kb.lock.Unlock()

	if len(kb.keys) == 0 {
		return 0, false
	}
	k := kb.keys[0]
	kb.keys = kb.keys[1:]
	return k, true
}

// liveCount counts how many alive neighbours a cell has
// world uses 255 for alive and 0 for dead
// edges wrap around like a donut (toroidal)
func liveCount(world [][]byte, y, x, h, w int) int {
	// eight neighbour offsets around a cell
	offsets := [8][2]int{
		{-1, -1}, {-1, 0}, {-1, 1},
		{0, -1}, {0, 1},
		{1, -1}, {1, 0}, {1, 1},
	}
	num := 0
	for _, off := range offsets {
		yy := (y + off[0] + h) % h
		xx := (x + off[1] + w) % w
		if world[yy][xx] == 255 {
			num++
		}
	}
	return num
}

// worker handles a rectangular chunk of the world
// it writes the new state into nextWorld and records which cells changed in flipped
func worker(
	nextWorld, world [][]byte,
	startY, startX, endY, endX, h, w, turn int,
	wg *sync.WaitGroup,
	flipped *[]util.Cell,
) {
	defer wg.Done()

	// make sure flipped slice exists so we can safely append
	if *flipped == nil {
		*flipped = make([]util.Cell, 0)
	}

	for y := startY; y < endY; y++ {
		for x := startX; x < endX; x++ {
			n := liveCount(world, y, x, h, w)

			switch {
			// alive cell with too few neighbours: dies from loneliness
			case world[y][x] == 255 && n < 2:
				nextWorld[y][x] = 0
				*flipped = append(*flipped, util.Cell{X: x, Y: y})

			// alive cell with 2 or 3 neighbours: stays alive, doing fine
			case world[y][x] == 255 && (n == 2 || n == 3):
				nextWorld[y][x] = 255

			// alive cell with too many neighbours: dies from overcrowding
			case world[y][x] == 255 && n > 3:
				nextWorld[y][x] = 0
				*flipped = append(*flipped, util.Cell{X: x, Y: y})

			// dead cell with exactly 3 neighbours: comes to life
			case world[y][x] == 0 && n == 3:
				nextWorld[y][x] = 255
				*flipped = append(*flipped, util.Cell{X: x, Y: y})

			// everything else is dead
			default:
				nextWorld[y][x] = 0
			}
		}
	}
}

// makeWorld builds a 2D slice with given height and width
// starts out all zeros (all dead cells)
func makeWorld(h, w int) [][]byte {
	world := make([][]byte, h)
	for i := range world {
		world[i] = make([]byte, w)
	}
	return world
}

// savePGM makes a copy of the world and writes it to a PGM file
// we copy under a mutex so nobody changes the world while we snapshot it
func savePGM(ioSys *ioState, hub *eventHub, world [][]byte, w, h, turn int, mu *sync.Mutex) {
	mu.Lock()
	copyWorld := makeWorld(h, w)
	for y := 0; y < h; y++ {
		copy(copyWorld[y], world[y])
	}
	mu.Unlock()

	// filename looks like WxHxTurn, e.g. 16x16x10
	filename := fmt.Sprintf("%vx%vx%v", w, h, turn)
	ioSys.ioWriteImage(filename, copyWorld)
	// tell the outside world we wrote a file
	hub.postEvent(ImageOutputComplete{CompletedTurns: turn, Filename: filename})
}

// advanceOneTurn runs one full generation of the game of life
// caller gives us current world + nextWorld buffers
func advanceOneTurn(p Params, world, nextWorld [][]byte, hub *eventHub, currentTurn int) {
	h := p.ImageHeight
	w := p.ImageWidth

	var wg sync.WaitGroup

	// one bucket per worker so they don't fight over a shared slice
	flipBuckets := make([][]util.Cell, p.Threads)

	workingH := h / p.Threads
	for i := 0; i < p.Threads; i++ {
		startY := i * workingH
		endY := startY + workingH
		// last worker takes any leftover rows
		if i == p.Threads-1 {
			endY = h
		}

		wg.Add(1)
		go worker(
			nextWorld, world,
			startY, 0, endY, w,
			h, w,
			currentTurn,
			&wg,
			&flipBuckets[i],
		)
	}

	wg.Wait()

	// after all workers are done, send out flip events
	// still keeps order: flips first, then TurnComplete later in distributor
	for i := 0; i < p.Threads; i++ {
		if len(flipBuckets[i]) > 0 {
			hub.postEvent(CellsFlipped{
				CompletedTurns: currentTurn,
				Cells:          flipBuckets[i],
			})
		}
	}
}

// countAlive just walks the world and counts how many cells are 255
func countAlive(world [][]byte) int {
	cnt := 0
	for y := range world {
		for x := range world[y] {
			if world[y][x] == 255 {
				cnt++
			}
		}
	}
	return cnt
}

// distributor is the central loop that runs the simulation
// it reacts to key presses, steps the world forward, and sends events
func distributor(p Params, ioSys *ioState, hub *eventHub, keys *keyBuffer) {
	h, w := p.ImageHeight, p.ImageWidth

	// build two buffers for double-buffering the world
	world := makeWorld(h, w)
	nextWorld := makeWorld(h, w)

	// load the initial world from disk
	ioSys.ioReadImage(fmt.Sprintf("%vx%v", w, h), world)

	// mu protects reads of world when we are saving or counting
	var mu sync.Mutex
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	// initial state setup
	turn := 0
	paused := false
	initialCells := make([]util.Cell, 0)
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			if world[y][x] == 255 {
				initialCells = append(initialCells, util.Cell{X: x, Y: y})
			}
		}
	}
	// tell UI which cells are alive at the start
	hub.postEvent(CellsFlipped{CompletedTurns: turn, Cells: initialCells})
	hub.postEvent(StateChange{CompletedTurns: turn, NewState: Executing})

	// ticker goroutine: every 2 seconds we report how many cells are alive
	doneTicker := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				alive := countAlive(world)
				currentTurn := turn
				mu.Unlock()
				hub.postEvent(AliveCellsCount{CompletedTurns: currentTurn, CellsCount: alive})
			case <-doneTicker:
				return
			}
		}
	}()

	// mainLoop is here so we can use "continue mainLoop" like old select default
mainLoop:
	for {
		// first, see if there is any key pressed waiting for us
		if key, ok := keys.grabKey(); ok {
			switch key {
			// "s" = save current world to file
			case 's':
				savePGM(ioSys, hub, world, w, h, turn, &mu)

			// "p" = toggle pause/resume
			case 'p':
				paused = !paused
				if paused {
					hub.postEvent(StateChange{CompletedTurns: turn, NewState: Paused})
				} else {
					hub.postEvent(StateChange{CompletedTurns: turn, NewState: Executing})
				}

			// "q" = do one more step, then quit nicely
			case 'q':
				// one last turn before we leave
				advanceOneTurn(p, world, nextWorld, hub, turn+1)

				mu.Lock()
				world = nextWorld
				turn++
				mu.Unlock()

				// gather list of living cells for final event
				aliveList := make([]util.Cell, 0)
				mu.Lock()
				for y := 0; y < h; y++ {
					for x := 0; x < w; x++ {
						if world[y][x] == 255 {
							aliveList = append(aliveList, util.Cell{X: x, Y: y})
						}
					}
				}
				mu.Unlock()

				hub.postEvent(FinalTurnComplete{CompletedTurns: turn, Alive: aliveList})
				savePGM(ioSys, hub, world, w, h, turn, &mu)
				hub.postEvent(StateChange{CompletedTurns: turn, NewState: Quitting})
				close(doneTicker)
				hub.shutDown()
				return
			}

			// after handling a key, go back to the start of the main loop
			continue mainLoop
		}

		// if paused, just chill and keep looping
		if paused {
			continue mainLoop
		}

		// if we've already done all turns, wrap up and exit
		if turn >= p.Turns {
			aliveList := make([]util.Cell, 0)
			mu.Lock()
			for y := 0; y < h; y++ {
				for x := 0; x < w; x++ {
					if world[y][x] == 255 {
						aliveList = append(aliveList, util.Cell{X: x, Y: y})
					}
				}
			}
			mu.Unlock()

			hub.postEvent(FinalTurnComplete{CompletedTurns: turn, Alive: aliveList})
			savePGM(ioSys, hub, world, w, h, turn, &mu)
			hub.postEvent(StateChange{CompletedTurns: turn, NewState: Quitting})
			close(doneTicker)
			hub.shutDown()
			return
		}

		// normal case: advance one turn
		advanceOneTurn(p, world, nextWorld, hub, turn+1)

		// swap buffers: nextWorld becomes the new world
		mu.Lock()
		world, nextWorld = nextWorld, world
		turn++
		currentTurn := turn
		mu.Unlock()

		// tell listeners that a turn finished
		hub.postEvent(TurnComplete{CompletedTurns: currentTurn})
	}
}
