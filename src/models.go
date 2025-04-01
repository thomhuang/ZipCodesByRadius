package main

import "github.com/dhconnelly/rtreego"

type RTree struct {
	Tree *rtreego.Rtree
}

type Logger struct {
	Records []string
	Length  int64
}

type Job struct {
	ZipCode string
	Lat     float64
	Lon     float64
	Rect    rtreego.Rect
}

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
