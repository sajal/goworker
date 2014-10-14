package disco

import (
	"archive/zip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func compileandzip(worker string) (string, error) {
	workerExe, err := compile(worker)
	if err != nil {
		return "", nil
	}
	defer os.Remove(workerExe)
	//Open this executable for reading
	exeFile, err := os.Open(workerExe)
	if err != nil {
		return "", nil
	}
	defer exeFile.Close()

	// create the zipfile
	zipfile, err := os.Create(workerExe + ".zip")
	if err != nil {
		return "", nil
	}
	defer zipfile.Close()

	// set w to write to zipfile
	w := zip.NewWriter(zipfile)

	f, err := w.Create("job")
	if err != nil {
		return "", nil
	}

	_, err = io.Copy(f, exeFile)
	if err != nil {
		return "", nil
	}

	err = w.Close()
	if err != nil {
		return "", nil
	}

	return workerExe + ".zip", nil
}

//Function compile builds source files if needed
//Cloned directly from jobpack/jobpack.go
//Note: If providing source file(s) go must be installed on the machine initializing the job.
func compile(worker string) (string, error) {
	pwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	var workerDir string
	//Create temp file for building executable into
	tf, err := ioutil.TempFile("", "")
	if err != nil {
		return "", err
	}

	exeFile := tf.Name()
	tf.Close()

	// Check if we have access to file or path, and it exists
	var fileStat os.FileInfo
	fileStat, err = os.Stat(worker)
	if err != nil {
		return "", err
	}

	if fileStat.IsDir() {
		workerDir = worker
		err = os.Chdir(workerDir)
		if err != nil {
			return "", err
		}
		buildMessages, err := exec.Command("go", "build", "-o", exeFile).CombinedOutput()
		if err != nil {
			return "", errors.New(fmt.Sprintf("%s\n%s", err, string(buildMessages)))
			//log.Fatal(string(buildMessages))
		}
	} else if strings.HasSuffix(worker, ".go") {
		var file string
		workerDir, file = filepath.Split(worker)
		if workerDir != "" {
			err = os.Chdir(workerDir)
			if err != nil {
				return "", err
			}
		}
		buildMessages, err := exec.Command("go", "build", "-o", exeFile, file).CombinedOutput()
		if err != nil {
			if err != nil {
				return "", errors.New(fmt.Sprintf("%s\n%s", err, string(buildMessages)))
			}
		}
	} else {
		// Is a file, is not a directory, fall back to executable
		//exeFile = worker
		//Still making a copy into temp. We will clean this temp file once zipped.
		copyFileContents(worker, exeFile)
	}
	err = os.Chdir(pwd)
	if err != nil {
		return "", err
	}
	return exeFile, nil
}

// copyFileContents copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file.
// http://stackoverflow.com/a/21067803/135625
func copyFileContents(src, dst string) (err error) {
	in, err := os.Open(src)
	if err != nil {
		return
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return
	}
	defer func() {
		cerr := out.Close()
		if err == nil {
			err = cerr
		}
	}()
	if _, err = io.Copy(out, in); err != nil {
		return
	}
	err = out.Sync()
	return
}
