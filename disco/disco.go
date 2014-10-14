//Package disco implements interface to talk to disco master/proxy.
//All jobs are submited thru this.
//
//Example workflow:-
//
//This creates a Disco object such that DISCO_HOST is localhost,
//DISCO_PORT is at 8999 and DISCO_PROXY is at http://127.0.0.1:8090.
//Blank string for proxy indicates no procy being used.
//	d, err := disco.NewDisco(&disco.DiscoOptions{"http://127.0.0.1:8090", "localhost", "8999", ""})
//Create new Job with prefix wordcount, go worker source "examples/count_words.go"
//and single input - http://discoproject.org/media/text/chekhov.txt
//	job := disco.NewJob("wordcount", "examples/count_words.go", []string{"http://discoproject.org/media/text/chekhov.txt"})
//Create a mapreduce JobPack.
//	jp, err := job.GetJobPack("mapreduce")
//Some manupilations will be made possible to JobPack in future...
//
//Submit JobPack to master
//	jr, err := d.SubmitJobPack(jp)
//Wait for results. In this case 1 minute is the maximum we are
//willing to wait for job to complete. Use nil to wait forever.
//	results, err := d.Wait(jr, time.Minute)
//Fetch a reader to read the actual results. 
//This returns an io.ReadCloser, parse as you wish...
//	rdr, err := d.Reader(results)
package disco

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	DEFAULT_POLL_INTERVAL = 2000
)

//The Disco type is responsible for interfacing with Disco
type Disco struct {
	client    *http.Client  //Underlying http.client
	options   *DiscoOptions //Options for disco
	masterurl string
}

//NewDisco initializes a Disco object based on options provided
func NewDisco(options *DiscoOptions) (*Disco, error) {
	var proxy *url.URL
	var err error
	disco := &Disco{options: options}
	//Ensure that the master url valid
	//If options.Proxy is not blank then it must be a valid URL.
	if options.Proxy != "" {
		proxy, err = url.Parse(options.Proxy)
		if err != nil {
			return nil, err
		}
		disco.client = &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxy)}}
	} else {
		disco.client = &http.Client{}
	}
	disco.masterurl = fmt.Sprintf("http://%s:%s", options.Masterhost, options.Masterport)
	_, err = url.Parse(disco.masterurl)
	if err != nil {
		return nil, err
	}

	return disco, nil
}

//Perform http post using underlying client
func (disco *Disco) Post(path string, bodyType string, data []byte) (*http.Response, error) {
	loc := disco.masterurl + path
	//Need a bytes.Reader otherwise disco fusses with 411 - Length Required
	resp, err := disco.client.Post(loc, bodyType, bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	return resp, nil
}

type rawresults struct {
}

func (disco *Disco) ProbeResult(jobname string) (*Results, error) {
	list := make([]interface{}, 2)
	list[0] = DEFAULT_POLL_INTERVAL //TODO: make this configurable
	list[1] = []string{jobname}
	json_jobname, err := json.Marshal(list)
	if err != nil {
		return nil, err
	}
	loc := "/disco/ctrl/get_results"
	resp, err := disco.Post(loc, "application/json", json_jobname)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		//No result yet, but no error also
		return nil, nil
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	log.Println(string(body))
	status, inputs, err := decode_response(body)
	if err != nil {
		return nil, err
	}
	if status == "active" {
		return nil, nil
	}

	//defer resp.Close()
	return &Results{inputs}, nil

}

func (disco *Disco) Wait(jr *JobResults, wait *time.Duration) (*Results, error) {
	for {
		res, err := disco.ProbeResult(jr.jobname)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
		//res and err both are nil means job is still running...
		//TODO: Check if timeout occured, if so then cause FAIL.
		//Sleep for the interval...
		time.Sleep(time.Duration(DEFAULT_POLL_INTERVAL) * time.Millisecond)
	}
}

//TODO
type JobResults struct {
	status     string
	jobname    string
	isfinished bool
}

//Wait until timeout. Send nil to wait infinitely

//The results object a list of strings
type Results struct {
	outputs []string
}

func (disco *Disco) convert_uri(uri string) string {
	scheme, locstr, path := loc_str(uri)
	// TODO add the dir scheme
	if scheme == "disco" {
		host, _ := getHostAndType(uri)
		if host != disco.options.Masterhost {
			return "http://" + locstr + ":" + disco.options.Masterport + "/" + path
		}
	}
	return uri
}

func (disco *Disco) http_reader(address string) (io.ReadCloser, error) {
	resp, err := disco.client.Get(address)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New(fmt.Sprintf("bad response: %s", resp.Status))
	}
	return resp.Body, nil
}

//TODO some form of io.Reader
func (disco *Disco) Reader(results *Results) (io.ReadCloser, error) {
	var readers []io.ReadCloser
	//TODO: Actually populate the readers
	for _, address := range results.outputs {
		address = disco.convert_uri(address)
		scheme, _ := SchemeSplit(address)
		switch scheme {
		case "http":
			fallthrough
		case "https":
			rdr, err := disco.http_reader(address)
			if err != nil {
				return nil, err
			}
			readers = append(readers, rdr)
		case "disco":
			return nil, errors.New("TODO: disco")
		case "dir":
			return nil, errors.New("TODO: dir")
		default:
			return nil, errors.New("Scheme not supported")
		}

	}
	return MultiReadCloser(readers...), nil
}

//Makes and submits the JobPack to master.
func (disco *Disco) SubmitJobPack(jp *JobPack) (*JobResults, error) {
	enc, err := jp.Encode()
	if err != nil {
		return nil, err
	}
	defer os.Remove(enc) //Clean up pack once submitted
	file, err := os.Open(enc)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, err
	}

	resp, err := disco.Post("/disco/job/new", "image/jpeg", data)
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		body, err := ioutil.ReadAll(resp.Body)
		if err == nil {
			log.Println(string(body))
		}
		return nil, errors.New("Task failed not 200")
	}
	defer resp.Body.Close()
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	result := []string{}
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}
	r := &JobResults{
		status:     result[0],
		jobname:    result[1],
		isfinished: false,
	}
	return r, nil

}

//The DiscoOptions defines the disco enviornment.
//Extend this if we need more things in here.
type DiscoOptions struct {
	Proxy      string //URL of DISCO_PROXY . Blank if no proxy needed
	Masterhost string
	Masterport string
	DiscoRoot  string
}

//NewDiscoOptionsEnv creates DiscoOptions by reading enviornment variables
func NewDiscoOptionsEnv() *DiscoOptions {
	return &DiscoOptions{
		Proxy: os.Getenv("DISCO_PROXY"),
		//		Masterurl: fmt.Sprintf("http://%s:%s", os.Getenv("DISCO_MASTER_HOST"), os.Getenv("DISCO_PORT")),
		Masterhost: os.Getenv("DISCO_MASTER_HOST"),
		Masterport: os.Getenv("DISCO_PORT"),
		DiscoRoot:  os.Getenv("DISCO_ROOT"),
	}
}

func decode_response(input []byte) (status string, results []string, err error) {
	result := make([]interface{}, 1)
	err = json.Unmarshal(input, &result)
	if err != nil {
		return
	}
	input0 := result[0].([]interface{})
	// jobname := input0[0].(string)

	result_list := input0[1].([]interface{})
	status = result_list[0].(string)
	inter0 := result_list[1].([]interface{})
	if len(inter0) == 0 {
		results = []string{""}
	} else {
		inter1 := inter0[0].([]interface{})
		results = make([]string, len(inter1))
		for i, item := range inter1 {
			results[i] = item.(string)
		}
	}
	return
}

func SchemeSplit(url string) (scheme, rest string) {
	if index := strings.Index(url, "://"); index == -1 {
		return "", url
	} else {
		return url[:index], url[index+len("://"):]
	}
}

func loc_str(url string) (scheme, locstr, path string) {
	scheme, rest := SchemeSplit(url)

	if index := strings.Index(rest, "/"); index == -1 {
		locstr = rest
		path = ""
	} else {
		locstr, path = rest[:index], rest[index+len("/"):]
	}
	return scheme, locstr, path
}

func getHostAndType(discoAddress string) (string, string) {
	_, rest := SchemeSplit(discoAddress)
	list := strings.Split(rest, "/")
	if len(list) < 2 {
		log.Fatal("disco path too short", list)
	}
	return list[0], list[1]
}
