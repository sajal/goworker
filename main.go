package main

//In active development. This will eventually become cli tool like the one jobpack provides

import (
	"fmt"
	"github.com/discoproject/goworker/disco"
	"log"
)

func main() {
	d, err := disco.NewDisco(&disco.DiscoOptions{"http://127.0.0.1:8090", "http://localhost:8999", ""})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(d)
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
}
