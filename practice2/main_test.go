package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/stretchr/testify/assert"
)

func post(t *testing.T, mux *http.ServeMux, url string, body []byte) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}

	req, err = http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func sendInsert(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	post(t, mux, "/insert", body)
}

func sendReplace(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	post(t, mux, "/replace", body)
}

func sendDelete(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	post(t, mux, "/delete", body)
}

func sendCheckpoint(t *testing.T, mux *http.ServeMux) {
	post(t, mux, "/checkpoint", []byte{})
}

func sendSelect(t *testing.T, mux *http.ServeMux) *geojson.FeatureCollection {
	body := make([]byte, 0)
	req, err := http.NewRequest("GET", "/select", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}

	req, err = http.NewRequest("GET", rr.Header().Get("location"), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
	collection, err := geojson.UnmarshalFeatureCollection(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func TestComplex(t *testing.T) {
	point := geojson.NewFeature(orb.Point{1, 1})
	point.ID = "1"

	line := geojson.NewFeature(orb.LineString{{2, 2}, {3, 3}})
	line.ID = "1"

	polygon := geojson.NewFeature(orb.Polygon{{{0, 0}, {3, 0}, {0, 4}, {0, 0}}})
	polygon.ID = "2"

	t.Cleanup(func() {
		os.Remove("test.wal")
	})
	t.Cleanup(func() {
		os.Remove("test.ckp")
	})
	t.Run("first", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		sendInsert(t, mux, point)
		collection := sendSelect(t, mux)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point.Geometry, collection.Features[0].Geometry)

		sendReplace(t, mux, line)
		collection = sendSelect(t, mux)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, line.Geometry, collection.Features[0].Geometry)
	})

	t.Run("second", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		collection := sendSelect(t, mux)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, line.Geometry, collection.Features[0].Geometry)

		sendDelete(t, mux, line)
		collection = sendSelect(t, mux)
		assert.Equal(t, 0, len(collection.Features))
	})

	t.Run("third", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		sendInsert(t, mux, polygon)
		sendCheckpoint(t, mux)
		sendInsert(t, mux, point)
		sendDelete(t, mux, point)
	})

	t.Run("fourth", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		collection := sendSelect(t, mux)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, polygon.Geometry, collection.Features[0].Geometry)
	})
}
