package main

import (
	"bytes"
	"fmt"
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

func sendSelect(t *testing.T, url string, min, max [2]float64) *geojson.FeatureCollection {
	resp, err := http.Get(url + "/select?rect=" + fmt.Sprint(min[0]) + "," + fmt.Sprint(min[1]) + "," + fmt.Sprint(max[0]) + "," + fmt.Sprint(max[1]))
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

	polygon := geojson.NewFeature(orb.Polygon{{{1, 1}, {2, 2}, {3, 3}, {1, 1}}})
	polygon.ID = "2"

	min := [2]float64{0.5, 0.5}
	max := [2]float64{5.5, 5.5}

	t.Cleanup(func() {
		os.Remove("test.wal")
		os.Remove("test.ckp")
	})
	t.Run("first", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, []Shard{{"test", []string{}}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL
		r.url = url

		sendInsert(t, url, point)
		collection := sendSelect(t, url, min, max)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point.Geometry, collection.Features[0].Geometry)

		sendReplace(t, url, line)
		collection = sendSelect(t, url, min, max)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, line.Geometry, collection.Features[0].Geometry)
	})

	t.Run("second", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, []Shard{{"test", []string{}}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL
		r.url = url

		collection := sendSelect(t, url, min, max)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, line.Geometry, collection.Features[0].Geometry)

		sendDelete(t, url, line)
		collection = sendSelect(t, url, min, max)
		assert.Equal(t, 0, len(collection.Features))
	})

	t.Run("third", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, []Shard{{"test", []string{}}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL
		r.url = url

		sendInsert(t, url, polygon)
		sendCheckpoint(t, url)
		sendInsert(t, url, point)
		sendDelete(t, url, point)
	})

	t.Run("fourth", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, []Shard{{"test", []string{}}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL
		r.url = url

		collection := sendSelect(t, url, min, max)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, polygon.Geometry, collection.Features[0].Geometry)
	})
}

func TestReplication(t *testing.T) {
	point := geojson.NewFeature(orb.Point{1, 1})
	point.ID = "1"

	line := geojson.NewFeature(orb.LineString{{2, 2}, {3, 3}})
	line.ID = "2"

	polygon := geojson.NewFeature(orb.Polygon{{{1, 1}, {2, 2}, {3, 3}, {1, 1}}})
	polygon.ID = "3"

	min := [2]float64{0.5, 0.5}
	max := [2]float64{5.5, 5.5}

	mux := http.NewServeMux()
	m := NewStorage(mux, "master", true)
	m.Run()

	s := NewStorage(mux, "slave", false)
	s.Run()

	r := NewRouter(mux, []Shard{{"master", []string{"slave"}}})
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
	r.url = url

	sendInsert(t, url, point)
	sendInsert(t, url, line)
	sendInsert(t, url, polygon)

	m.AddReplica(url, "slave")

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := sendSelect(t, url, min, max)
		assert.Equal(t, 3, len(collection.Features))
	}

	sendDelete(t, url, point)
	sendDelete(t, url, line)
	sendDelete(t, url, polygon)

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := sendSelect(t, url, min, max)
		assert.Equal(t, 0, len(collection.Features))
	}
}

func TestSharding(t *testing.T) {
	point1 := geojson.NewFeature(orb.Point{1, 1})
	point1.ID = "1"

	point2 := geojson.NewFeature(orb.Point{-1, 1})
	point2.ID = "2"

	point3 := geojson.NewFeature(orb.Point{2, 2})
	point3.ID = "3"

	point4 := geojson.NewFeature(orb.Point{-2, 2})
	point4.ID = "4"

	mux := http.NewServeMux()
	m1 := NewStorage(mux, "master1", true)
	m1.Run()

	s1 := NewStorage(mux, "slave1", false)
	s1.Run()

	m2 := NewStorage(mux, "master2", true)
	m2.Run()

	s2 := NewStorage(mux, "slave2", false)
	s2.Run()

	r := NewRouter(mux, []Shard{{"master1", []string{"slave1"}}, {"master2", []string{"slave2"}}})
	r.Run()

	t.Cleanup(func() {
		r.Stop()
		s1.Stop()
		m1.Stop()
		s2.Stop()
		m2.Stop()
		os.Remove("master1.wal")
		os.Remove("slave1.wal")
		os.Remove("master1.ckp")
		os.Remove("slave1.ckp")
		os.Remove("master2.wal")
		os.Remove("slave2.wal")
		os.Remove("master2.ckp")
		os.Remove("slave2.ckp")
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	url := server.URL
	r.url = url

	sendInsert(t, url, point1)
	sendInsert(t, url, point2)
	sendInsert(t, url, point3)
	sendInsert(t, url, point4)

	m1.AddReplica(url, "slave1")
	m2.AddReplica(url, "slave2")

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := sendSelect(t, url, [2]float64{0.5, 0.5}, [2]float64{5.5, 5.5})
		assert.Equal(t, 2, len(collection.Features))
		collection = sendSelect(t, url, [2]float64{-5.5, 0.5}, [2]float64{-0.5, 5.5})
		assert.Equal(t, 2, len(collection.Features))
	}

	sendDelete(t, url, point1)
	sendDelete(t, url, point3)

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := sendSelect(t, url, [2]float64{0.5, 0.5}, [2]float64{5.5, 5.5})
		assert.Equal(t, 0, len(collection.Features))
		collection = sendSelect(t, url, [2]float64{-5.5, 0.5}, [2]float64{-0.5, 5.5})
		assert.Equal(t, 2, len(collection.Features))
	}
}
