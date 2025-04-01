package main

import (
	"github.com/dhconnelly/rtreego"
	"github.com/umahmood/haversine"
	"math"
	"runtime"
	"sync"
)

var logger Logger

func main() {
	fileContents, err := getPostalCodeGeography()
	if err != nil {
		OutputLogFile()
		return
	}

	// dim = 2D for our radius between points
	// min nodes = 1
	// max nodes = inf
	rt := RTree{
		Tree: rtreego.NewTree(2, 1, math.MaxInt32),
	}

	// Initialize Map of ZipCode <--> ZipCode MetaData
	postalCodeMap := make(map[string]*PostalCodeItem)

	// Build the spatial index
	for _, p := range fileContents.PostalCodes {
		rt.create(p, postalCodeMap)
	}

	numWorkers := runtime.NumCPU() * 4 // Start with 4x cores for I/O bound work

	var wg sync.WaitGroup
	// prevent blocking when there's a temporary imbalance between producers and consumers
	jobs := make(chan Job, numWorkers*2)
	results := make(chan Pair, numWorkers*2)

	// Launch worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		// goroutine to find intersection
		go func() {
			defer wg.Done()

			for job := range jobs {
				nearby := rt.Tree.SearchIntersect(job.Rect)
				var nearbyZips []string
				for _, item := range nearby {
					// cast from `Spatial` to our struct
					pcItem := item.(*PostalCodeItem)

					// Skip extra logic if it's the same postal code
					if pcItem.ZipCode == job.ZipCode {
						nearbyZips = append(nearbyZips, pcItem.ZipCode)
						continue
					}

					// Calculate exact distance using Haversine so it's radius vs square
					coord1 := haversine.Coord{Lat: job.Lat, Lon: job.Lon}
					coord2 := haversine.Coord{Lat: pcItem.Lat, Lon: pcItem.Lon}
					_, km := haversine.Distance(coord1, coord2)
					if km <= 25 {
						nearbyZips = append(nearbyZips, pcItem.ZipCode)
					}
				}

				// recall the goroutine that writes to our csv when resultChan is ready, i.e. has data
				// we are essentially sending the pair to the channel as it listens
				results <- Pair{ZipCode: job.ZipCode, NearbyZipCodes: nearbyZips}
			}
		}()
	}

	// Allows parallel collection of results while processing continues ...
	// We only close the results channel after all our workers terminate
	// and closing the channel signals the main thread when ALL results are processed
	go func() {
		wg.Wait()
		close(results)
	}()

	// allows feeding jobs while processing happens
	// closing the jobs channel signals when no more jobs are incoming
	go func() {
		for zc, metadata := range postalCodeMap {
			jobs <- Job{
				ZipCode: zc,
				Lat:     metadata.Lat,
				Lon:     metadata.Lon,
				Rect:    *metadata.Rect,
			}
		}
		close(jobs)
	}()

	// Collect results
	zipMap := make(map[string][]string)
	for res := range results {
		zipMap[res.ZipCode] = res.NearbyZipCodes
	}

	OutputResults(zipMap)
}
