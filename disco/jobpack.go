package disco

import (
	"encoding/binary"
	"encoding/json"
	"github.com/discoproject/goworker/jobutil"
	"io"
	"io/ioutil"
	"os"
	"os/user"
)

const (
	MAGIC       = 0xd5c0 << 16
	MAGIC_MASK  = 0xffff << 16
	VERSION_1   = 0x0001
	VERSION_2   = 0x0001
	HEADER_SIZE = 128
)

//Type JobPack involves creating of zip archive to submit to Disco
type JobPack struct {
	jobdict map[string]interface{}
	jobenv  map[string]interface{}
	jobhome string
	jobdata string
	version int
	worker  string
}

func NewJobPack() *JobPack {
	jp := &JobPack{}
	jp.jobdict = make(map[string]interface{})
	jp.jobenv = make(map[string]interface{})
	return jp
}

func (jp *JobPack) AddToJobDict(key string, value interface{}) {
	jp.jobdict[key] = value
}

func (jp *JobPack) AddToJobEnv(key string, value interface{}) {
	jp.jobenv[key] = value
}

//Encode compile the binary and encodes the payload to send to Disco
func (jp *JobPack) Encode() (string, error) {
	zipFileName, err := compileandzip(jp.worker)
	if err != nil {
		return "", nil
	}
	defer os.Remove(zipFileName) //Get rid of zipped executable
	job_dict, err := json.Marshal(jp.jobdict)
	if err != nil {
		return "", nil
	}
	job_dict_len := len(job_dict)
	job_env, err := json.Marshal(jp.jobenv)
	if err != nil {
		return "", nil
	}
	job_env_len := len(job_env)
	zipfile, err := os.Open(zipFileName)
	if err != nil {
		return "", nil
	}
	defer zipfile.Close()

	fileinfo, err := zipfile.Stat()
	if err != nil {
		return "", nil
	}
	jobHomeSize := int(fileinfo.Size())
	var header Header
	header.MV = uint32(MAGIC + jp.version)
	header.JobDictOffset = uint32(HEADER_SIZE)
	header.JobEnvOffset = uint32(HEADER_SIZE + job_dict_len)
	header.JobHomeOffset = uint32(HEADER_SIZE + job_dict_len + job_env_len)
	header.JobDataOffset = uint32(HEADER_SIZE + job_dict_len + job_env_len + jobHomeSize)

	file, err := ioutil.TempFile("", "")
	if err != nil {
		return "", nil
	}
	err = binary.Write(file, binary.BigEndian, header)
	if err != nil {
		return "", nil
	}
	binary.Write(file, binary.BigEndian, job_dict)
	binary.Write(file, binary.BigEndian, job_env)

	io.Copy(file, zipfile)
	file.Close()

	return file.Name(), nil
}

//Type Job defines a Job to be run
type Job struct {
	Inputs []string //List of inputs
	Name   string
	Worker string //Directory, .go file or an executable
	//Future: Choose options like number of reducers, etc
}

func NewJob(name, worker string, inputs []string) Job {
	if name == "" {
		//Automatically set name if none set
		name = "gojob"
	}
	return Job{inputs, name, worker}
}

//Get and create JobPack. jobtype can be mapreduce or pipeline
func (job *Job) GetJobPack(jobtype string) (*JobPack, error) {
	jp := NewJobPack()
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	user, err := user.Current()
	if err != nil {
		return nil, err
	}
	jp.AddToJobDict("prefix", job.Name)
	jp.AddToJobDict("owner", user.Username+"@"+host)
	//FIXME: Some options below are hardcoded.
	//These should be configurable
	jp.AddToJobDict("scheduler", make(map[string]string))
	jp.AddToJobDict("save_info", "ddfs")
	jp.AddToJobDict("worker", "./job")
	jp.AddToJobDict("nr_reduces", 2)
	jp.AddToJobDict("save_results", false)
	jp.AddToJobDict("input", getEffectiveInputs(job.Inputs))

	if jobtype == "mapreduce" {
		jp.AddToJobDict("reduce?", true)
		jp.AddToJobDict("map?", true)
		jp.version = VERSION_1
	} else {
		jp.version = VERSION_2
	}
	jp.worker = job.Worker
	return jp, nil
}

//Fixme: No idea whats going on here...
func getEffectiveInputs(inputs []string) []string {
	effectiveInputs := make([]string, 0)

	for _, input := range inputs {
		if scheme, rest := jobutil.SchemeSplit(input); scheme == "tag" {
			urls := jobutil.GetUrls(rest)
			for _, url := range urls {
				// TODO we are using only the first replica here
				effectiveInputs = append(effectiveInputs, url[0])
			}
		} else {
			effectiveInputs = append(effectiveInputs, input)
		}
	}
	return effectiveInputs
}

type Header struct {
	MV            uint32
	JobDictOffset uint32
	JobEnvOffset  uint32
	JobHomeOffset uint32
	JobDataOffset uint32
	_             [27]uint32
}
