package gol

// Params describes some basic settings for running the game
// (like how big the picture is and how long we run it).
// nothing fancy here, just a tiny config bag
type Params struct {
	Turns       int
	Threads     int
	ImageWidth  int
	ImageHeight int
}

// pumpEvents takes events from our internal hub and pushes them out
// into the public channel so the outside world can see what's happening.
// this runs in its own goroutine so it doesn’t block game logic.
func pumpEvents(hub *eventHub, out chan<- Event) {
	go func() {
		for {
			hub.lock.Lock()

			// wait until there is at least one event OR the hub is closing
			for len(hub.pending) == 0 && !hub.isClosed {
				hub.wakeUp.Wait()
			}

			// if there’s nothing left AND we’re closed, time to leave
			if len(hub.pending) == 0 && hub.isClosed {
				hub.lock.Unlock()
				close(out)
				return
			}

			// take first event from queue
			ev := hub.pending[0]
			hub.pending = hub.pending[1:]
			hub.lock.Unlock()

			// forward it to the outside world
			out <- ev
		}
	}()
}

// watchKeys listens for key presses coming from outside
// and stores them inside our key buffer so distributor can read them later.
// basically just a simple bridge between a channel and our own queue.
func watchKeys(buf *keyBuffer, keyPresses <-chan rune) {
	go func() {
		for k := range keyPresses {
			// store each new key in our little buffer
			buf.pushKey(k)
		}
	}()
}

// Run starts everything up: IO system, event system, key listener,
// and finally the distributor which drives the whole simulation.
// this function is kinda like turning on the engine.
func Run(p Params, events chan<- Event, keyPresses <-chan rune) {
	// set up IO subsystem (shared state + worker goroutine)
	ioSys := newIOState(p)
	go startIo(ioSys)

	// set up the event hub and start forwarding events
	hub := newEventHub()
	pumpEvents(hub, events)

	// set up keyboard buffer and goroutine that fills it
	keysBuf := newKeyBuffer()
	watchKeys(keysBuf, keyPresses)

	// jump into main game loop which handles the actual simulation
	distributor(p, ioSys, hub, keysBuf)
}
