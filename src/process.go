package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/dhconnelly/rtreego"
)

// Create Item within RTree for a given Geography
func (rt *RTree) create(geo Geography, postalCodeMap map[string]*PostalCodeItem) {
	point := rtreego.Point{geo.Longitude, geo.Latitude}
	// create 15mi x 15mi / 25km x 25km search rectangle in tree
	rect, _ := rtreego.NewRect(point, []float64{0.25, 0.25})
	// maintain postal code metadata
	item := &PostalCodeItem{
		Rect:    &rect,
		ZipCode: geo.ZipCode,
		Lat:     geo.Latitude,
		Lon:     geo.Longitude,
	}
	// insert our current geo in our r-tree
	rt.Tree.Insert(item)
	// update our map
	postalCodeMap[geo.ZipCode] = item
}

func processPostalCodeFile(reader io.Reader) []Geography {
	var postalCodes []Geography

	// read tsv content from file reader
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
			logger.Append(fmt.Sprintf("could not read record from US.txt: %s\n", err.Error()))
			continue
		}

		postalCode := record[1]
		city := record[2]
		stateCode := record[4]

		latitude, err := strconv.ParseFloat(record[9], 64)
		if err != nil {
			logger.Append(fmt.Sprintf("Could not parse latitude for given record %s: %s\n", postalCode, err.Error()))
			continue
		}

		longitude, err := strconv.ParseFloat(record[10], 64)
		if err != nil {
			logger.Append(fmt.Sprintf("Could not parse longitude for given record %s: %s\n", postalCode, err.Error()))
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

func OutputResults(zipMap map[string]string, timeTaken time.Duration) {
	jsonData, err := json.MarshalIndent(zipMap, "", "  ")
	if err != nil {
		logger.Append(fmt.Sprintf("could not serialize zipcode json data: %s\n", err.Error()))
		return
	}

	// Create or open a file for writing
	jsonFile, err := os.Create("./NearbyZipCodes.json")
	if err != nil {
		logger.Append(fmt.Sprintf("could not create json output file: %s\n", err.Error()))
		return
	}
	defer jsonFile.Close()

	// Write the JSON data to the file
	_, err = jsonFile.Write(jsonData)
	if err != nil {
		logger.Append(fmt.Sprintf("could not write zipcode data to json: %s\n", err.Error()))
		return
	}

	logger.Append(fmt.Sprintf("Time Taken:%s", timeTaken))
	logger.Append("Outputted file successfully")

	OutputLogFile()
}
