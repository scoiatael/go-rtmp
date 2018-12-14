package main

import (
	"flag"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/yutopp/go-rtmp"
)

func connect(dst string) (*rtmp.Stream, error) {
	silentLogger := log.New()
	silentLogger.SetLevel(log.WarnLevel)

	client, err := rtmp.Dial("rtmp", dst, &rtmp.ConnConfig{
		Logger: silentLogger,
	})
	if err != nil {
		return nil, err
	}
	defer client.Close()

	if err := client.Connect("ql"); err != nil {
		return nil, err
	}

	stream, err := client.CreateStream()
	if err != nil {
		return nil, err
	}

	return stream, nil
}

func openStream(dst string, wg *sync.WaitGroup, ch chan<- error) {
	defer wg.Done()

	for {
		stream, err := connect(dst)
		ch <- err
		if err == nil {
			time.Sleep(time.Minute)

			stream.Close()
			return // Loop on error
		}
	}
}

func readResults(expected int, wg *sync.WaitGroup, ch <-chan error) {
	defer wg.Done()

	results := 0
	errors := 0
	total := 0

	ticker := time.NewTicker(time.Second)
	for {
		select {
		case err := <-ch:
			if err != nil {
				errors += 1
			} else {
				results += 1
				total += 1
				if total == expected {
					log.Infof("OK: %04d, Err: %04d, total: %04d", results, errors, total)
					return
				}
			}
		case <-ticker.C:
			log.Infof("OK: %04d, Err: %04d, total: %04d", results, errors, total)
			results = 0
			errors = 0
		}
	}
}

func main() {
	var conns = flag.Int("conns", 1000, "number of connections to open")
	var dst = flag.String("dst", "localhost:11935", "RTMP server address")
	flag.Parse()

	var wg sync.WaitGroup
	results := make(chan error)
	total := *conns

	for i := 0; i < total; i++ {
		wg.Add(1)
		go openStream(*dst, &wg, results)
	}

	wg.Add(1)
	go readResults(total, &wg, results)
	wg.Wait()
}
