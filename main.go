package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"

	"github.com/dgraph-io/badger"
	"github.com/dgraph-io/badger/pb"
)

type Edge struct {
	From int64 `json:"from"`
	To   int64 `json:"to"`
}

func main() {

	var dir = flag.String("folder", "/tmp/badger", "folder where your badger db lives")
	flag.Parse()

	log.Println("using db at", *dir)

	db, err := badger.Open(badger.DefaultOptions(*dir))
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	accounts := make(map[int64]map[int64]interface{})

	stream := db.NewStream()

	stream.Send = func(list *pb.KVList) error {
		for _, kv := range list.GetKv() {
			var e Edge
			err = json.Unmarshal(kv.GetValue(), &e)
			if err != nil {
				log.Fatal(err)
			}

			var to map[int64]interface{}
			var ok bool
			to, ok = accounts[e.From]
			if !ok {
				to = make(map[int64]interface{})
			}
			to[e.To] = nil
			accounts[e.From] = to
		}
		return nil
	}

	if err := stream.Orchestrate(context.Background()); err != nil {
		log.Fatal(err)
	}
	log.Println("Num accounts:", len(accounts))

	degreeDistribution := make(map[int]int)

	for _, tos := range accounts {
		degree := len(tos)
		d := degreeDistribution[degree]
		d += 1
		degreeDistribution[degree] = d
	}

	min := 10
	max := 0
	for k, _ := range degreeDistribution {
		if k < min {
			min = k
		}
		if k > max {
			max = k
		}
	}
	for d := min; d <= max; d++ {
		count, ok := degreeDistribution[d]
		if ok {
			log.Println(d, count)
		}
	}

}
