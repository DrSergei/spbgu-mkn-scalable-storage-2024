package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/stretchr/testify/assert"
)

func post(t *testing.T, url string, body []byte) {
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", resp.StatusCode, http.StatusOK)
	}
}

func sendInsert(t *testing.T, url string, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	post(t, url+"/insert", body)
}

func sendReplace(t *testing.T, url string, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	post(t, url+"/replace", body)
}

func sendDelete(t *testing.T, url string, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	post(t, url+"/delete", body)
}

func sendCheckpoint(t *testing.T, url string) {
	post(t, url+"/checkpoint", []byte{})
}

func sendSelect(t *testing.T, url string) *geojson.FeatureCollection {
	resp, err := http.Get(url + "/select")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", resp.StatusCode, http.StatusOK)
	}
	json, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := geojson.UnmarshalFeatureCollection(json)
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func TestPersistence(t *testing.T) {
	point := geojson.NewFeature(orb.Point{1, 1})
	point.ID = "1"

	line := geojson.NewFeature(orb.LineString{{2, 2}, {3, 3}})
	line.ID = "1"

	polygon := geojson.NewFeature(orb.Polygon{{{0, 0}, {3, 0}, {0, 4}, {0, 0}}})
	polygon.ID = "2"

	t.Cleanup(func() {
		os.Remove("test.wal")
		os.Remove("test.ckp")
	})
	t.Run("first", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL

		sendInsert(t, url, point)
		collection := sendSelect(t, url)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point.Geometry, collection.Features[0].Geometry)

		sendReplace(t, url, line)
		collection = sendSelect(t, url)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, line.Geometry, collection.Features[0].Geometry)
	})

	t.Run("second", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL

		collection := sendSelect(t, url)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, line.Geometry, collection.Features[0].Geometry)

		sendDelete(t, url, line)
		collection = sendSelect(t, url)
		assert.Equal(t, 0, len(collection.Features))
	})

	t.Run("third", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL

		sendInsert(t, url, polygon)
		sendCheckpoint(t, url)
		sendInsert(t, url, point)
		sendDelete(t, url, point)
	})

	t.Run("fourth", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL

		collection := sendSelect(t, url)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, polygon.Geometry, collection.Features[0].Geometry)
	})
}

func TestReplication(t *testing.T) {
	point := geojson.NewFeature(orb.Point{1, 1})
	point.ID = "1"

	line := geojson.NewFeature(orb.LineString{{2, 2}, {3, 3}})
	line.ID = "2"

	polygon := geojson.NewFeature(orb.Polygon{{{0, 0}, {3, 0}, {0, 4}, {0, 0}}})
	polygon.ID = "3"

	mux := http.NewServeMux()
	m := NewStorage(mux, "master", true)
	m.Run()

	s := NewStorage(mux, "slave", false)
	s.Run()

	r := NewRouter(mux, [][]string{{"master", "slave"}})
	r.Run()

	t.Cleanup(func() {
		r.Stop()
		s.Stop()
		m.Stop()
		os.Remove("master.wal")
		os.Remove("slave.wal")
		os.Remove("master.ckp")
		os.Remove("slave.ckp")
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	url := server.URL

	sendInsert(t, url, point)
	sendInsert(t, url, line)
	sendInsert(t, url, polygon)

	m.AddReplica(url, "slave")

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := sendSelect(t, url)
		assert.Equal(t, 3, len(collection.Features))
	}

	sendDelete(t, url, point)
	sendDelete(t, url, line)
	sendDelete(t, url, polygon)

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := sendSelect(t, url)
		assert.Equal(t, 0, len(collection.Features))
	}
}
