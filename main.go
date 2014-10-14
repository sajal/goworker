package main

//In active development. This will eventually become cli tool like the one jobpack provides

import (
	"bufio"
	"fmt"
	"github.com/discoproject/goworker/disco"
	"log"
)

func main() {
	d, err := disco.NewDisco(&disco.DiscoOptions{"http://127.0.0.1:8090", "localhost", "8999", ""})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(d)
	//Maybe club job and jobpack as single object...
	job := disco.NewJob("wordcount", "examples/count_words.go", []string{"http://discoproject.org/media/text/chekhov.txt"})
	fmt.Println(job)
	jp, err := job.GetJobPack("mapreduce")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(jp)
	jr, err := d.SubmitJobPack(jp)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(jr)
	results, err := d.Wait(jr, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(results)
	rdr, err := d.Reader(results)
	if err != nil {
		log.Fatal(err)
	}
	defer rdr.Close()
	fmt.Println(rdr)
	scanner := bufio.NewScanner(rdr)
	for scanner.Scan() {
		text := scanner.Text()
		fmt.Println(text)
	}

}
