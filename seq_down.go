package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"strconv"
	"strings"
)

type urls []string

// String is the method to format the flag's value, part of the flag.Value interface.
// The String method's output will be used in diagnostics.
func (i *urls) String() string {
	return fmt.Sprint(*i)
}

// Set is the method to set the flag value, part of the flag.Value interface.
// Set's argument is a string to be parsed to set the flag.
// It's a comma-separated list, so we split it.
func (i *urls) Set(value string) error {
	// If we wanted to allow the flag to be set multiple times,
	// accumulating values, we would delete this if statement.
	// That would permit usages such as
	//	-deltaT 10s -deltaT 15s
	// and other combinations.
	//if len(*i) > 0 {
	//		return errors.New("urls flag already set")
	//	}
	for _, dt := range strings.Split(value, " ") {
		*i = append(*i, dt)
	}
	return nil
}

var urlFlag urls

type urlData struct {
	url          string
	filename     string
	fileContents []byte
}

func produceURL(out chan urlData) {
	var to_download_url urlData

	re := regexp.MustCompile("(?P<path>.+/)(?P<filename>\\D+)(?P<count>\\d+)(?P<ext>\\..+$)")

	for i := 0; i < len(urlFlag); i++ {
		img_url := urlFlag[i]

		u, err := url.Parse(img_url)
		if err != nil {
			log.Fatal(err)
		}
		n1 := re.SubexpNames()
		result := re.FindStringSubmatch(u.Path)

		md := map[string]string{}
		for j, n := range result {
			fmt.Printf("%d. match='%s'\tname='%s'\n", j, n, n1[j])
			md[n1[j]] = n
		}

		path, filename, count, ext := md["path"], md["filename"], md["count"], md["ext"]

		var count_i uint64
		if count_i, err = strconv.ParseUint(count, 10, 16); err != nil {
			log.Fatal(err)
		}
		var j uint64
		for j = 0; j <= count_i; j++ {
			filenamePic := "%s%0" + strconv.Itoa(len(count)) + "d%s"
			new_url := fmt.Sprintf("%s"+filenamePic, path, filename, j, ext)
			u.Path = new_url
			fmt.Println("generated", u)
			to_download_url.url = u.String()
			to_download_url.filename = fmt.Sprintf(filenamePic, filename, j, ext)

			res, err := http.Get(u.String())
			if err != nil {
				log.Fatal(err)
			}
			if res.StatusCode == 200 {
				to_download_url.fileContents, err = ioutil.ReadAll(res.Body)
				if err != nil {
					log.Fatal(err)
				}

				out <- to_download_url
			} else {
				fmt.Println("HTTP", res.StatusCode, u)
			}
			res.Body.Close()
		}
		close(out)
	}
}

func consumeURL(in chan urlData, done chan bool) {
	for itm := range in {
		if err := ioutil.WriteFile(itm.filename, itm.fileContents, 0600); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d", len(itm.fileContents))
	}

	done <- true
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Define a flag to accumulate urls. Because it has a special type,
	// we need to use the Var function and therefore create the flag during
	// init.
	flag.Var(&urlFlag, "url", "url[] to download")
	queue := flag.Int("q", 4, "channel queue size")
	flag.Parse()

	if flag.NFlag() == 0 {
		flag.PrintDefaults()
	} else {
		urlChan := make(chan urlData, *queue)
		doneChan := make(chan bool)

		go produceURL(urlChan)
		go consumeURL(urlChan, doneChan)
		<-doneChan
	}
}
