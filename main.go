package main

import (
	"archive/zip"
	"bytes"
	"encoding/csv"
	"github.com/dhconnelly/rtreego"
	"github.com/umahmood/haversine"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type Response struct {
	PostalCodes []Geography
}

type Geography struct {
	ZipCode   string
	City      string
	StateCode string
	Latitude  float64
	Longitude float64
}

type Pair struct {
	ZipCode        string
	NearbyZipCodes []string
}

type PostalCodeItem struct {
	Rect    *rtreego.Rect
	ZipCode string
	Lat     float64
	Lon     float64
}

func (p *PostalCodeItem) Bounds() rtreego.Rect {
	return *p.Rect
}

func main() {
	fileContents, err := getPostalCodeGeography()
	if err != nil {
		log.Fatal(err)
		return
	}

	// dim = 2D for our radius between points
	// min nodes = 1
	// max nodes = inf
	rt := rtreego.NewTree(2, 1, math.MaxInt32)
	postalCodeMap := make(map[string]*PostalCodeItem)

	// Build the spatial index
	for _, p := range fileContents.PostalCodes {
		point := rtreego.Point{p.Longitude, p.Latitude}
		// rect, but essentially storing points
		rect, _ := rtreego.NewRect(point, []float64{0.01, 0.01})
		item := &PostalCodeItem{
			Rect:    &rect,
			ZipCode: p.ZipCode,
			Lat:     p.Latitude,
			Lon:     p.Longitude,
		}
		// insert our current geo in our r-tree
		rt.Insert(item)
		postalCodeMap[p.ZipCode] = item
	}

	// Process in parallel,
	numWorkers := runtime.NumCPU()
	// determines # of postal codes for a single worker, taking the floor of numWorkers
	chunkSize := (len(fileContents.PostalCodes) + numWorkers - 1) / numWorkers

	var wg sync.WaitGroup
	resultChan := make(chan Pair, 1) // Buffered channel

	// Create and open file
	file, err := os.Create("geos.csv")
	if err != nil {
		log.Fatal("Could not create file:", err)
		return
	}
	defer file.Close()

	// ready for writing
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Start a goroutine (parallelization) to write results as they come in
	go func() {
		for pair := range resultChan {
			writer.Write([]string{pair.ZipCode, strings.Join(pair.NearbyZipCodes, ",")})
		}
	}()

	// Launch worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		// chunk size for a worker given how many workers have been "queued"
		start := i * chunkSize
		end := start + chunkSize
		if end > len(fileContents.PostalCodes) {
			end = len(fileContents.PostalCodes)
		}

		// goroutine to find intersection
		go func(start, end int) {
			defer wg.Done()

			for i := start; i < end; i++ {
				p := fileContents.PostalCodes[i]

				// Calculate approximate bounding box for 25km
				// ~0.25 degrees at mid-latitudes is roughly 25km
				searchRadius := 0.25
				searchPoint := rtreego.Point{p.Longitude, p.Latitude}
				// "rectangle" to find intersection between other stored points in tree
				searchRect, _ := rtreego.NewRect(searchPoint, []float64{searchRadius, searchRadius})

				// Find potential nearby postal codes within our search rectangle
				nearbyItems := rt.SearchIntersect(searchRect)

				var nearbyZips []string
				for _, item := range nearbyItems {
					// cast from `Spatial` to our struct
					pcItem := item.(*PostalCodeItem)

					// Skip extra logic if it's the same postal code
					if pcItem.ZipCode == p.ZipCode {
						nearbyZips = append(nearbyZips, pcItem.ZipCode)
						continue
					}

					// Calculate exact distance using Haversine so it's radius vs square
					coord1 := haversine.Coord{Lat: p.Latitude, Lon: p.Longitude}
					coord2 := haversine.Coord{Lat: pcItem.Lat, Lon: pcItem.Lon}
					_, km := haversine.Distance(coord1, coord2)
					if km <= 25 {
						nearbyZips = append(nearbyZips, pcItem.ZipCode)
					}
				}

				// recall the goroutine that writes to our csv when resultChan is ready, i.e. has data
				// we are essentially sending the pair to the channel as it listens
				resultChan <- Pair{ZipCode: p.ZipCode, NearbyZipCodes: nearbyZips}
			}
		}(start, end)
	}

	// Wait for all workers to finish, then close the result channel
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Main goroutine waits for the result channel to be closed
	// which happens after all workers are done
	for range resultChan {
		// Just drain the channel
	}
}

func getPostalCodeGeography() (response Response, err error) {
	// Try to use cached file first
	cacheFile := "US.zip"
	var zipReader *zip.ReadCloser

	if _, err := os.Stat(cacheFile); err == nil {
		// Use cached file
		zipReader, err = zip.OpenReader(cacheFile)
		if err != nil {
			return Response{}, err
		}
		defer zipReader.Close()
	} else {
		// Download the file
		resp, err := http.Get("https://download.geonames.org/export/zip/US.zip")
		if err != nil {
			return Response{}, err
		}
		defer resp.Body.Close()

		// Save to cache
		buf := &bytes.Buffer{}
		_, err = io.Copy(buf, resp.Body)
		if err != nil {
			return Response{}, err
		}

		// Save to disk for future use
		if err := os.WriteFile(cacheFile, buf.Bytes(), 0644); err != nil {
			log.Println("Warning: couldn't save cache file:", err)
		}

		b := bytes.NewReader(buf.Bytes())
		r, err := zip.NewReader(b, int64(b.Len()))
		if err != nil {
			return Response{}, err
		}

		// Process the zip file
		var postalCodes []Geography
		for _, f := range r.File {
			if f.FileHeader.Name != "US.txt" {
				continue
			}

			rc, err := f.Open()
			if err != nil {
				return Response{}, err
			}
			defer rc.Close()

			postalCodes = processPostalCodeFile(rc)
			break
		}

		return Response{PostalCodes: postalCodes}, nil
	}

	// Process the zip file from disk, ignore `readme.txt`
	// essentially only process for `US.txt`
	var postalCodes []Geography
	for _, f := range zipReader.File {
		if f.Name != "US.txt" {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			return Response{}, err
		}
		defer rc.Close()

		postalCodes = processPostalCodeFile(rc)
		break
	}

	return Response{PostalCodes: postalCodes}, nil
}

func processPostalCodeFile(reader io.Reader) []Geography {
	var postalCodes []Geography

	// read csv from file reader
	csvReader := csv.NewReader(reader)
	csvReader.Comma = '\t' // Set the delimiter to tab
	csvReader.FieldsPerRecord = 12

	// Read the file line by line
	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Println("Warning: error reading record:", err)
			continue
		}

		postalCode := record[1]
		city := record[2]
		stateCode := record[4]

		latitude, err := strconv.ParseFloat(record[9], 64)
		if err != nil {
			log.Println("Warning: invalid latitude for", postalCode)
			continue
		}

		longitude, err := strconv.ParseFloat(record[10], 64)
		if err != nil {
			log.Println("Warning: invalid longitude for", postalCode)
			continue
		}

		currGeography := Geography{
			ZipCode:   postalCode,
			City:      city,
			StateCode: stateCode,
			Latitude:  latitude,
			Longitude: longitude,
		}

		postalCodes = append(postalCodes, currGeography)
	}

	return postalCodes
}
