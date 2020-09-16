// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

// Scroll illustrates scrolling through a set of documents.
//
// Example
//
// Scroll through an index called "products".
// Use "_uid" as the default field:
//
//     scroll -index=products -size=100
//
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync/atomic"
	"time"

	"github.com/olivere/elastic/v7"
	"golang.org/x/sync/errgroup"
	"gopkg.in/yaml.v2"
)

var (
	configfile string
	cnf        Config
	vBuild     string
	vVersion   string
)

type Response map[string]interface{}

type Config struct {
	Elastic struct {
		Hosts  []string `yaml:"hosts"`
		Fields []string `yaml:"fields"`
		SSL    bool     `yaml:"ssl"`
		Cert   string   `yaml:"certfile"`
		Index  string   `yaml:"index"`
		Query  string   `yaml:"query"`
	} `yaml:"elastic"`
}

func configParse(f string) Config {
	var c Config
	yamlFile, err := ioutil.ReadFile(f)
	if err != nil {
		panic(err)
	}

	err = yaml.Unmarshal(yamlFile, &c)
	if err != nil {
		panic(err)
	}

	if c.Elastic.Index == "" {
		panic("Index not set")
	}

	if len(c.Elastic.Hosts) == 0 {
		c.Elastic.Hosts[0] = "http://127.0.0.1:9200/"
	}

	if c.Elastic.Query == "" {
		panic("Query is empty")
	}

	return c
}

func init() {
	flag.StringVar(&configfile, "config", "main.yml", "Read configuration from this file")
	flag.StringVar(&configfile, "f", "main.yml", "Read configuration from this file")
	flag.Parse()
	cnf = configParse(configfile)
}

func main() {
	log.SetFlags(0)
	ehosts := cnf.Elastic.Hosts
	// Create an Elasticsearch client
	client, err := elastic.NewClient(
		elastic.SetURL(ehosts...),
		elastic.SetSniff(true),
		elastic.SetHealthcheckInterval(10*time.Second),
		elastic.SetGzip(true),
	)

	if err != nil {
		log.Fatal(err)
	}

	// Setup a group of goroutines from the excellent errgroup package
	g, ctx := errgroup.WithContext(context.TODO())

	// Hits channel will be sent to from the first set of goroutines and consumed by the second
	type hit struct {
		Slice int
		Hit   elastic.SearchHit
	}
	hitsc := make(chan hit)

	//begin := time.Now()

	// Start goroutine for this sliced scroll
	g.Go(func() error {
		defer close(hitsc)
		svc := client.Scroll(cnf.Elastic.Index).Body(cnf.Elastic.Query)
		for {
			res, err := svc.Do(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}
			for _, searchHit := range res.Hits.Hits {
				// Pass the hit to the hits channel, which will be consumed below
				select {
				case hitsc <- hit{Hit: *searchHit}:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		}
		return nil
	})

	// Second goroutine will consume the hits sent from the workers in first set of goroutines
	var total uint64
	var aResp []Response

	g.Go(func() error {
		for hit := range hitsc {
			atomic.AddUint64(&total, 1)
			var src Response
			if err := json.Unmarshal(hit.Hit.Source, &src); err != nil {
				return err
			}
			aResp = append(aResp, src)
			select {
			default:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})

	// Wait until all goroutines are finished
	if err := g.Wait(); err != nil {
		log.Fatal(err)
	}

	for i := range aResp {
		fmt.Printf("%s,%s\n", aResp[i][cnf.Elastic.Fields[0]], aResp[i][cnf.Elastic.Fields[1]])
	}
	log.Printf("Scrolled through a total of %d documents\n", total)
}
