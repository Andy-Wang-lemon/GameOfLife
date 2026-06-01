package main

import (
	"fmt"
	"io"
	"log"
	"testing"

	"uk.ac.bris.cs/gameoflife/gol"
)

// BenchmarkGol
// Standard Go benchmark: each iteration runs one full Game of Life simulation.
// No manual timing or averaging; everything is handled by testing + benchstat.
func BenchmarkGol(b *testing.B) {
	// Prevent log output from polluting benchmark results
	log.SetOutput(io.Discard)

	const (
		width  = 512
		height = 512
		turns  = 1000
	)

	// Worker counts we want to compare
	threadsList := []int{1, 2, 4, 8, 16}

	for _, threads := range threadsList {
		threads := threads // Avoid closure capturing the outer variable

		// Sub-benchmark name (similar to "1_workers" in the handout)
		name := fmt.Sprintf("%dx%dx%d-%d_threads", width, height, turns, threads)

		b.Run(name, func(b *testing.B) {
			// Fixed parameters: each sub-benchmark uses the same set of params
			p := gol.Params{
				Turns:       turns,
				Threads:     threads,
				ImageWidth:  width,
				ImageHeight: height,
			}

			// If there is any one-off initialization, call it before:
			// b.ResetTimer()

			for i := 0; i < b.N; i++ {
				events := make(chan gol.Event)
				go gol.Run(p, events, nil)

				// Consume all events until Run finishes and closes the channel
				for range events {
				}
			}
		})
	}
}

