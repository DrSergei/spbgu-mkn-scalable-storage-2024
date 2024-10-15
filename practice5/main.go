package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Shard struct {
	leader   string
	replicas []string
}

type Router struct {
	index  rtree.RTreeGN[float64, int]
	shards []Shard
	url    string
	mtx    sync.RWMutex
}

func getRectangle(params url.Values) ([2]float64, [2]float64, error) {
	get := params.Get("rect")
	rect := strings.Split(get, ",")
	if len(rect) < 4 {
		return [2]float64{}, [2]float64{}, errors.New("expected 4 floats")
	}
	minx, err := strconv.ParseFloat(rect[0], 64)
	if err != nil {
		return [2]float64{}, [2]float64{}, err
	}
	miny, err := strconv.ParseFloat(rect[1], 64)
	if err != nil {
		return [2]float64{}, [2]float64{}, err
	}
	maxx, err := strconv.ParseFloat(rect[2], 64)
	if err != nil {
		return [2]float64{}, [2]float64{}, err
	}
	maxy, err := strconv.ParseFloat(rect[3], 64)
	if err != nil {
		return [2]float64{}, [2]float64{}, err
	}
	min := [2]float64{minx, miny}
	max := [2]float64{maxx, maxy}
	return min, max, nil
}

func (rt *Router) getPostHandler(action string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		rt.mtx.RLock()
		defer rt.mtx.RUnlock()
		json, err := io.ReadAll(r.Body)
		if err != nil {
			writeError(w, err)
			return
		}
		f, _, err := getFeature(json)
		if err != nil {
			writeError(w, err)
			return
		}
		rt.index.Search(f.Geometry.Bound().Min, f.Geometry.Bound().Min, func(min, max [2]float64, data int) bool {
			shard := rt.shards[data]
			http.Redirect(w, r, "/"+shard.leader+"/"+action, http.StatusTemporaryRedirect)
			return false
		})
	}
}

func (rt *Router) addShard(shard Shard) {
	rt.mtx.Lock()
	defer rt.mtx.Unlock()
	n := 0
	wg := sync.WaitGroup{}
	for i := -180; i < 180; i += 10 {
		for j := -180; j < 180; j += 10 {
			var min [2]float64 = [2]float64{float64(j), float64(i)}
			var max [2]float64 = [2]float64{float64(j + 10), float64(i + 10)}
			if n%(len(rt.shards)+1) == 0 {
				m := n % len(rt.shards)
				rt.index.Delete(min, max, m)
				rt.index.Insert(min, max, len(rt.shards))
				wg.Add(1)
				go func() {
					// select
					collection := geojson.NewFeatureCollection()
					res, _ := http.Get(rt.url + "/" + rt.shards[m].leader + "/select?rect=" + fmt.Sprint(min[0]) + "," + fmt.Sprint(min[1]) + "," + fmt.Sprint(max[0]) + "," + fmt.Sprint(max[1]))
					json, _ := io.ReadAll(res.Body)
					tmp := geojson.NewFeatureCollection()
					tmp.UnmarshalJSON(json)
					for _, f := range tmp.Features {
						collection.Append(f)
						data, _ := f.MarshalJSON()
						// delete
						http.Post(rt.url+"/"+rt.shards[m].leader+"/delete", "application/json", bytes.NewReader(data))
						// insert
						http.Post(rt.url+"/"+shard.leader+"/insert", "application/json", bytes.NewReader(data))
					}
					wg.Done()
				}()
			}
			n++
		}
	}
	wg.Wait()
	rt.shards = append(rt.shards, shard)
}

func NewRouter(mux *http.ServeMux, shards []Shard) *Router {
	result := Router{}
	result.shards = shards
	n := 0
	for i := -180; i < 180; i += 10 {
		for j := -180; j < 180; j += 10 {
			var min [2]float64 = [2]float64{float64(j), float64(i)}
			var max [2]float64 = [2]float64{float64(j + 10), float64(i + 10)}
			result.index.Insert(min, max, n)
			n++
			n %= len(shards)
		}
	}
	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))
	mux.HandleFunc("/select", func(w http.ResponseWriter, r *http.Request) {
		result.mtx.RLock()
		defer result.mtx.RUnlock()
		min, max, err := getRectangle(r.URL.Query())
		if err != nil {
			writeError(w, err)
			return
		}
		collection := geojson.NewFeatureCollection()
		set := make(map[*Shard]bool)
		result.index.Search(min, max, func(min, max [2]float64, data int) bool {
			shard := &result.shards[data]
			if set[shard] {
				return false
			}
			set[shard] = true
			n := rand.Intn(len(shard.replicas) + 1)
			var res *http.Response
			if n == 0 {
				res, _ = http.Get(result.url + "/" + shard.leader + "/select?" + r.URL.RawQuery)
			} else {
				n--
				res, _ = http.Get(result.url + "/" + shard.replicas[n] + "/select?" + r.URL.RawQuery)
			}
			json, _ := io.ReadAll(res.Body)
			tmp := geojson.NewFeatureCollection()
			tmp.UnmarshalJSON(json)
			for _, f := range tmp.Features {
				collection.Append(f)
			}
			return true
		})
		answer, err := collection.MarshalJSON()
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(answer)
	})
	mux.HandleFunc("/insert", result.getPostHandler("insert"))
	mux.HandleFunc("/replace", result.getPostHandler("replace"))
	mux.HandleFunc("/delete", result.getPostHandler("delete"))
	mux.HandleFunc("/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		result.mtx.RLock()
		defer result.mtx.RUnlock()
		for _, shard := range result.shards {
			http.Post(result.url+"/"+shard.leader+"/checkpoint", "application/json", bytes.NewReader([]byte{}))
			for _, replica := range shard.replicas {
				http.Post(result.url+"/"+replica+"/checkpoint", "application/json", bytes.NewReader([]byte{}))
			}
		}
	})
	return &result
}

func (r *Router) Run()  {}
func (r *Router) Stop() {}

type Message struct {
	action string
	data   []byte
	params url.Values
	cb     chan any
}

type Transaction struct {
	Action  string
	Feature *geojson.Feature
}

type Storage struct {
	name     string
	replicas []*websocket.Conn
	leader   bool
	ctx      context.Context
	cancel   context.CancelFunc
	queue    chan Message
	features map[string]*geojson.Feature
	index    rtree.RTree
	mtx      sync.Mutex
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func handleRequest(action string, queue chan Message, r *http.Request) (any, error) {
	slog.Info("Handle " + action)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	cb := make(chan any, 1)
	queue <- Message{action, body, r.URL.Query(), cb}
	res := <-cb
	close(cb)
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func handlePostRequest(action string, queue chan Message, w http.ResponseWriter, r *http.Request) {
	_, err := handleRequest(action, queue, r)
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func NewStorage(mux *http.ServeMux, name string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan Message, 256)
	result := Storage{name, make([]*websocket.Conn, 0), leader, ctx, cancel, queue, make(map[string]*geojson.Feature), rtree.RTree{}, sync.Mutex{}}
	mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		res, err := handleRequest("select", queue, r)
		if err != nil {
			writeError(w, err)
			return
		}
		collection, ok := res.(*geojson.FeatureCollection)
		if !ok {
			writeError(w, err)
			return
		}
		json, err := collection.MarshalJSON()
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(json)
	})
	if leader {
		mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
			handlePostRequest("insert", queue, w, r)
		})
		mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
			handlePostRequest("replace", queue, w, r)
		})
		mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
			handlePostRequest("delete", queue, w, r)
		})
	} else {
		mux.HandleFunc("/"+name+"/replication", func(w http.ResponseWriter, r *http.Request) {
			var upgrader = websocket.Upgrader{
				ReadBufferSize:  1024,
				WriteBufferSize: 1024,
			}
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				panic(err)
			}
			defer ws.Close()
			for {
				_, message, err := ws.ReadMessage()
				if err != nil {
					if err, ok := err.(*websocket.CloseError); ok && (err.Code == websocket.CloseNormalClosure) {
						return
					}
					panic(err)
				}
				transaction := Transaction{}
				err = json.Unmarshal(message, &transaction)
				if err != nil {
					panic(err)
				}
				body, err := transaction.Feature.MarshalJSON()
				if err != nil {
					panic(err)
				}
				cb := make(chan any, 1)
				queue <- Message{transaction.Action, body, nil, cb}
				<-cb
			}
		})
		mux.HandleFunc("/"+name+"/checkpoint", func(w http.ResponseWriter, r *http.Request) {
			handlePostRequest("checkpoint", queue, w, r)
		})
	}
	return &result
}

func getFeature(data []byte) (*geojson.Feature, string, error) {
	f, err := geojson.UnmarshalFeature(data)
	if err != nil {
		slog.Error("Failed to unmarshal body")
		return nil, "", err
	}
	id, ok := f.ID.(string)
	if !ok {
		slog.Error("Failed to get id")
		return nil, "", errors.New("id should be string")
	}
	return f, id, nil
}

func (s *Storage) makeCheckpoint() error {
	collection := geojson.NewFeatureCollection()
	for _, f := range s.features {
		collection.Append(f)
	}
	json, err := collection.MarshalJSON()
	if err != nil {
		return err
	}
	err = os.WriteFile(s.name+".ckp", json, 0666)
	if err != nil {
		return err
	}
	err = os.WriteFile(s.name+".wal", []byte{}, 0666)
	return err
}

func (s *Storage) makeTransaction(action string, f *geojson.Feature) error {
	transaction := Transaction{action, f}
	json, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	wal, err := os.OpenFile(s.name+".wal", os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer wal.Close()
	_, err = wal.Write(json)
	return err
}

func (s *Storage) init() {
	defer s.makeCheckpoint()
	wal, err := os.ReadFile(s.name + ".wal")
	if err != nil {
		return
	}
	ckp, err := os.ReadFile(s.name + ".ckp")
	if err == nil {
		collection, err := geojson.UnmarshalFeatureCollection(ckp)
		if err != nil {
			panic(err)
		}
		for _, f := range collection.Features {
			s.features[f.ID.(string)] = f
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(wal))
	for decoder.More() {
		transaction := Transaction{}
		err := decoder.Decode(&transaction)
		if err != nil {
			panic(err)
		}
		switch transaction.Action {
		case "insert":
			s.features[transaction.Feature.ID.(string)] = transaction.Feature
		case "replace":
			s.features[transaction.Feature.ID.(string)] = transaction.Feature
		case "delete":
			delete(s.features, transaction.Feature.ID.(string))
		}
	}
	for _, f := range s.features {
		s.index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Min, f)
	}
}

func (s *Storage) replicateAction(action string, f *geojson.Feature) error {
	transaction := Transaction{action, f}
	json, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	for _, conn := range s.replicas {
		err = conn.WriteMessage(websocket.TextMessage, json)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) replicateState(conn *websocket.Conn) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	for _, f := range s.features {
		transaction := Transaction{"insert", f}
		json, err := json.Marshal(transaction)
		if err != nil {
			panic(err)
		}
		err = conn.WriteMessage(websocket.TextMessage, json)
		if err != nil {
			panic(err)
		}
	}
}

func (s *Storage) AddReplica(url string, name string) {
	if !s.leader {
		panic("Only leader can have replics")
	}
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(url, "http")+"/"+name+"/replication", nil)
	if err != nil {
		panic(err)
	}
	s.replicas = append(s.replicas, conn)
	s.replicateState(conn)
}

func (s *Storage) handleMessage(msg Message) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	switch msg.action {
	case "select":
		collection := geojson.NewFeatureCollection()
		min, max, err := getRectangle(msg.params)
		if err != nil {
			msg.cb <- err
			return
		}
		s.index.Search(min, max, func(min, max [2]float64, data interface{}) bool {
			collection.Append(data.(*geojson.Feature))
			return true
		})
		msg.cb <- collection
	case "insert":
		f, id, err := getFeature(msg.data)
		if err != nil {
			msg.cb <- err
			return
		}
		err = s.makeTransaction("insert", f)
		if err != nil {
			msg.cb <- err
			return
		}
		s.index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Min, f)
		s.features[id] = f
		msg.cb <- nil
		if s.leader {
			s.replicateAction(msg.action, f)
		}
	case "replace":
		f, id, err := getFeature(msg.data)
		if err != nil {
			msg.cb <- err
			return
		}
		err = s.makeTransaction("replace", f)
		if err != nil {
			msg.cb <- err
			return
		}
		old, ok := s.features[id]
		if !ok {
			msg.cb <- errors.New(id + " not exists")
		}
		s.index.Delete(old.Geometry.Bound().Min, old.Geometry.Bound().Min, old)
		s.index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Min, f)
		s.features[id] = f
		msg.cb <- nil
		if s.leader {
			s.replicateAction(msg.action, f)
		}
	case "delete":
		f, id, err := getFeature(msg.data)
		if err != nil {
			msg.cb <- err
			return
		}
		err = s.makeTransaction("delete", f)
		if err != nil {
			msg.cb <- err
			return
		}
		if s.features[id] == nil {
			msg.cb <- nil
			return
		}
		s.index.Delete(s.features[id].Geometry.Bound().Min, s.features[id].Geometry.Bound().Min, s.features[id])
		delete(s.features, id)
		msg.cb <- nil
		if s.leader {
			s.replicateAction(msg.action, f)
		}
	case "checkpoint":
		msg.cb <- s.makeCheckpoint()
	default:
		panic("unsopperted action")
	}
}

func (s *Storage) Run() {
	if s.leader {
		s.init()
	} else {
		_, err := os.Create(s.name + ".wal")
		if err != nil {
			panic(err)
		}
	}
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case msg := <-s.queue:
				s.handleMessage(msg)
			}
		}
	}()
}

func (s *Storage) Stop() {
	s.cancel()
	for _, conn := range s.replicas {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}
}

func main() {
	mux := http.NewServeMux()
	shard := Shard{"master", []string{"slave"}}
	r := NewRouter(mux, []Shard{shard})
	s := NewStorage(mux, "slave", false)
	m := NewStorage(mux, "master", true)
	server := http.Server{}
	server.Addr = "127.0.0.1:8080"
	r.url = "127.0.0.1:8080"
	server.Handler = mux

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		slog.Info("Listen http://" + server.Addr)
		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Info("err", "err", err)
		}
		wg.Done()
	}()
	time.Sleep(5 * time.Second)
	m.AddReplica(server.Addr, "slave")

	r.Run()
	defer r.Stop()
	s.Run()
	defer s.Stop()
	m.Run()
	defer m.Stop()

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		slog.Info("Got signal " + sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()
	defer slog.Info("Stopped")
	wg.Wait()
}
