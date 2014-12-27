package disco

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/discoproject/goworker/jobutil"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

//Go style interface . mostly ripped out of github.com/discoproject/goworker/worker

type MapOut struct {
	Value []byte
	Label int
}

type DiscoWorker struct {
	Map      func(io.Reader, chan MapOut, map[string]interface{})
	Reduce   func(io.Reader, io.Writer, map[string]interface{})
	Sort     bool                                    //Is it needed to be sorted before Reduce?
	Sortfn   func(io.Reader) io.ReadCloser           //Send nil to use default sort command
	Outputfn func(io.Reader, map[string]interface{}) //Optional task on outputs post reduce
}

func (worker *DiscoWorker) Run() {
	var w Worker

	if worker.Sortfn == nil {
		worker.Sortfn = jobutil.Sorted
	}
	w.discoworker = worker
	send_worker()
	w.task = request_task()
	job_dict := w.task.ExtractJobDict()
	params, ok := job_dict["params"]
	debug("params", params)
	if ok {
		//We have some data in params...
		w.params = params.(map[string]interface{})
	}
	jobutil.SetKeyValue("HOST", w.task.Host)
	master, port := jobutil.HostAndPort(w.task.Master)
	jobutil.SetKeyValue("DISCO_MASTER_HOST", master)
	if port != fmt.Sprintf("%d", w.task.Disco_port) {
		panic("port mismatch: " + port)
	}
	jobutil.SetKeyValue("DISCO_PORT", port)
	jobutil.SetKeyValue("PUT_PORT", string(w.task.Put_port))
	jobutil.SetKeyValue("DISCO_DATA", w.task.Disco_data)
	jobutil.SetKeyValue("DDFS_DATA", w.task.Ddfs_data)

	w.inputs = request_input()

	pwd, err := os.Getwd()
	Check(err)

	if w.task.Stage == "map" {
		w.runMapStage(pwd, "map_out_")
	} else if w.task.Stage == "map_shuffle" {
		w.outputs = make([]*Output, len(w.inputs))
		for i, input := range w.inputs {
			w.outputs[i] = new(Output)
			w.outputs[i].output_location = input.replica_location
			w.outputs[i].label = input.label
			w.outputs[i].output_size = 0 // TODO find a way to calculate the size
		}
	} else if w.task.Stage == "reduce_shuffle" {
		w.runReduceShuffleStage(pwd, "reduce_shuffle_")
	} else {
		if worker.Reduce == nil {
			w.outputs = make([]*Output, len(w.inputs))
			for i, input := range w.inputs {
				w.outputs[i] = new(Output)
				w.outputs[i].output_location = input.replica_location
				w.outputs[i].label = input.label
				w.outputs[i].output_size = 0 // TODO find a way to calculate the size
			}
		} else {
			w.runReduceStage(pwd, "reduce_out_")

		}
	}

	send_output(w.outputs)
	request_done()
}

type Worker struct {
	task        *Task
	inputs      []*Input
	outputs     []*Output
	discoworker *DiscoWorker
	params      map[string]interface{} //Future: Need to figure out how to pass this...
}

const (
	DEBUG = true
)

func Check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func debug(prefix string, msg interface{}) {
	if DEBUG {
		file, err := os.OpenFile("/tmp/debug", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
		Check(err)
		defer file.Close()
		fmt.Fprintf(file, "%s: %v\n", prefix, msg)
	}
}

func send(key string, payload interface{}) {
	enc, err := json.Marshal(payload)
	if err != nil {
		panic("could not encode")
	}
	str := fmt.Sprintf("%s %d %s\n", key, len(enc), enc)
	fmt.Printf(str)
	debug("send", str)
}

func recv() (string, int, []byte) {
	var size int
	var status string
	fmt.Scanf("%s %d", &status, &size)
	reader := bufio.NewReader(os.Stdin)
	input := make([]byte, size)
	io.ReadFull(reader, input)
	debug("recv", fmt.Sprintf("%d ", size)+string(input))
	return status, size, input
}

func send_worker() {
	type WorkerMsg struct {
		Pid     int    `json:"pid"`
		Version string `json:"version"`
	}
	wm := WorkerMsg{os.Getpid(), "1.1"}
	send("WORKER", wm)

	_, _, response := recv()
	if string(response) != "\"ok\"" {
		panic(response)
	}
}

func request_task() *Task {
	task := new(Task)
	send("TASK", "")
	_, _, line := recv()
	json.Unmarshal(line, &task)
	debug("info", task)
	return task
}

func request_input() []*Input {
	send("INPUT", "")
	_, _, line := recv()
	return process_input(line)
}

func process_input(jsonInput []byte) []*Input {
	var mj []interface{}

	json.Unmarshal(jsonInput, &mj)
	flag := mj[0].(string)
	if flag != "done" {
		// TODO support multiple passes for inputs
		panic(flag)
	}
	_inputs := mj[1].([]interface{})
	result := make([]*Input, len(_inputs))
	for index, rawInput := range _inputs {
		inputTuple := rawInput.([]interface{})

		id := inputTuple[0].(float64)
		status := inputTuple[1].(string)

		label := -1
		switch t := inputTuple[2].(type) {
		case string:
			label = -1
		case float64:
			label = int(t)
		}
		_replicas := inputTuple[3].([]interface{})

		replicas := _replicas[0].([]interface{})

		//FIXME avoid conversion to float when reading the item
		replica_id := replicas[0].(float64)
		replica_location := replicas[1].(string)

		debug("info", fmt.Sprintln(id, status, label, replica_id, replica_location))

		input := new(Input)
		input.id = int(id)
		input.status = status
		input.label = label
		input.replica_id = int(replica_id)
		input.replica_location = replica_location
		result[index] = input
	}
	return result
}

func send_output(outputs []*Output) {
	for _, output := range outputs {
		v := make([]interface{}, 3)
		v[0] = output.label
		v[1] = output.output_location //"http://example.com"
		v[2] = output.output_size

		send("OUTPUT", v)
		_, _, line := recv()
		debug("info", string(line))
	}
}

func request_done() {
	send("DONE", "")
	_, _, line := recv()
	debug("info", string(line))
}

type Task struct {
	Host       string
	Master     string
	Jobname    string
	Taskid     int
	Stage      string
	Grouping   string
	Group      string
	Disco_port int
	Put_port   int
	Disco_data string
	Ddfs_data  string
	Jobfile    string
}

type Input struct {
	id               int
	status           string
	label            int
	replica_id       int
	replica_location string
}

type Output struct {
	label           int
	output_location string
	output_size     int64
}

func (t *Task) ExtractJobDict() map[string]interface{} {
	file, err := os.Open(t.Jobfile)
	Check(err)
	var header Header
	err = binary.Read(file, binary.BigEndian, &header)
	Check(err)
	job_dict_buf := make([]byte, header.JobEnvOffset-header.JobDictOffset)
	err = binary.Read(file, binary.BigEndian, &job_dict_buf)
	Check(err)
	job_dict := make(map[string]interface{})
	err = json.Unmarshal(job_dict_buf, &job_dict)
	Check(err)
	return job_dict
}

func (w *Worker) runMapStage(pwd string, prefix string) {
	outputs := make(map[int]*os.File)
	var err error

	locations := make([]string, len(w.inputs))
	for i, input := range w.inputs {
		locations[i] = input.replica_location
	}

	readCloser := jobutil.AddressReader(locations, jobutil.Setting("DISCO_DATA"))
	chanout := make(chan MapOut, 10)

	go w.discoworker.Map(readCloser, chanout, w.params)
	for item := range chanout {
		_, ok := outputs[item.Label]
		if !ok {
			outputs[item.Label], err = ioutil.TempFile(pwd, prefix)
			Check(err)
			defer outputs[item.Label].Close()
		}
		fmt.Fprintf(outputs[item.Label], "%s\n", item.Value)
	}
	readCloser.Close()

	w.outputs = make([]*Output, len(outputs))
	absDiscoPath, err := filepath.EvalSymlinks(w.task.Disco_data)
	Check(err)
	i := 0
	for label, output := range outputs {
		fileinfo, err := output.Stat()
		Check(err)
		w.outputs[i] = new(Output)
		w.outputs[i].output_location =
			"disco://" + jobutil.Setting("HOST") + "/disco/" + output.Name()[len(absDiscoPath)+1:]
		w.outputs[i].output_size = fileinfo.Size()
		w.outputs[i].label = label
		i++
	}
}

func (w *Worker) runReduceShuffleStage(pwd string, prefix string) {
	output, err := ioutil.TempFile(pwd, prefix)
	output_name := output.Name()
	Check(err)
	defer output.Close()
	locations := make([]string, len(w.inputs))
	for i, input := range w.inputs {
		locations[i] = input.replica_location
	}

	readCloser := jobutil.AddressReader(locations, jobutil.Setting("DISCO_DATA"))

	_, err = io.Copy(output, readCloser)
	Check(err)

	readCloser.Close()

	fileinfo, err := output.Stat()
	Check(err)

	w.outputs = make([]*Output, 1)
	w.outputs[0] = new(Output)

	absDiscoPath, err := filepath.EvalSymlinks(w.task.Disco_data)
	Check(err)
	w.outputs[0].output_location =
		"disco://" + jobutil.Setting("HOST") + "/disco/" + output_name[len(absDiscoPath)+1:]
	w.outputs[0].output_size = fileinfo.Size()
}

func (w *Worker) runReduceStage(pwd string, prefix string) {
	output, err := ioutil.TempFile(pwd, prefix)
	output_name := output.Name()
	Check(err)
	defer output.Close()
	locations := make([]string, len(w.inputs))
	for i, input := range w.inputs {
		locations[i] = input.replica_location
	}

	readCloser := jobutil.AddressReader(locations, jobutil.Setting("DISCO_DATA"))

	if w.discoworker.Sort {
		realin := w.discoworker.Sortfn(readCloser)
		w.discoworker.Reduce(realin, output, w.params)
		realin.Close()
	} else {
		w.discoworker.Reduce(readCloser, output, w.params)
	}

	readCloser.Close()
	if w.discoworker.Outputfn != nil {
		//Sync it cause we are re-reading...
		output.Sync()
		reader, err := os.Open(output_name)
		Check(err)
		defer reader.Close()
		w.discoworker.Outputfn(reader, w.params)

	}
	fileinfo, err := output.Stat()
	Check(err)

	w.outputs = make([]*Output, 1)
	w.outputs[0] = new(Output)

	absDiscoPath, err := filepath.EvalSymlinks(w.task.Disco_data)
	Check(err)
	w.outputs[0].output_location =
		"disco://" + jobutil.Setting("HOST") + "/disco/" + output_name[len(absDiscoPath)+1:]
	w.outputs[0].output_size = fileinfo.Size()
}

func GetParam(params map[string]interface{}, name string) string {
	f, ok := params[name]
	if !ok {
		log.Fatal(errors.New(fmt.Sprintf("%s param missing?!?!?!?", name)))
	}
	fname, ok := f.(string)
	if !ok {
		log.Fatal(errors.New(fmt.Sprintf("%s param not string?!?!?!", name)))
	}
	return fname
}
