package main

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"net/http"
)

func getPostalCodeGeography() (response Response, err error) {
	// Download the file, always get latest, don't cache
	resp, err := http.Get("https://download.geonames.org/export/zip/US.zip")
	if err != nil {
		logger.Append(fmt.Sprintf("could not download zipcode data: %s\n", err.Error()))
		return Response{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logger.Append(fmt.Sprintf("could not read zipped zipcode data response body: %s\n", err.Error()))
		return Response{}, err
	}

	zipReader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if zipReader == nil || err != nil {
		logger.Append(fmt.Sprintf("could not unzip zipcode data from response body: %s\n", err.Error()))
		return Response{}, err
	}

	// Process the zip file
	var postalCodes []Geography
	for _, f := range zipReader.File {
		if f.FileHeader.Name != "US.txt" {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			logger.Append(fmt.Sprintf("could not open US.txt file from unzipped response: %s\n", err.Error()))
			return Response{}, err
		}
		defer rc.Close()

		postalCodes = processPostalCodeFile(rc)
		break
	}

	return Response{PostalCodes: postalCodes}, nil
}
