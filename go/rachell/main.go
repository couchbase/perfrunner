package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
)

const (
	baseURL = "http://%s/settings/rbac/users/local/%s"
)

var (
	numUsers, numWorkers     int
	address, admin, password string
	httpClient               *http.Client
)

func newBody() io.Reader {
	form := url.Values{}
	form.Add("roles", "data_writer[*],data_reader[*],query_select[*]")
	form.Add("password", "Pa$$w0rd")

	return strings.NewReader(form.Encode())
}

func addUser(user string) error {
	log.Printf("Adding a new user: %s\n", user)

	req, err := http.NewRequest("PUT", fmt.Sprintf(baseURL, address, user), newBody())
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(admin, password)

	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(ioutil.Discard, resp.Body)

	if resp.StatusCode != 200 {
		return fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	return nil
}

func addUsers(users <-chan string, results chan<- error) {
	for u := range users {
		err := addUser(u)
		if err != nil {
			results <- err
			return
		}
	}
	results <- nil
}

func newUsers() <-chan string {
	users := make(chan string, 2*numWorkers)

	go func() {
		defer close(users)

		for i := 0; i < numUsers; i++ {
			users <- fmt.Sprintf("user-%d", i)
		}
	}()

	return users
}

func init() {
	t := &http.Transport{MaxIdleConnsPerHost: numWorkers}
	httpClient = &http.Client{Transport: t}
}

func main() {
	flag.IntVar(&numUsers, "users", 30000, "Number of users to create")
	flag.IntVar(&numWorkers, "workers", 500, "Number of workers to use")

	flag.StringVar(&address, "address", "127.0.0.1:8091", "Target address")
	flag.StringVar(&admin, "admin", "Administrator", "Administrator username")
	flag.StringVar(&password, "password", "password", "Administrator password")

	flag.Parse()

	users := newUsers()
	results := make(chan error, numWorkers)

	for w := 0; w < numWorkers; w++ {
		go addUsers(users, results)
	}

	for w := 0; w < numWorkers; w++ {
		r := <-results
		if r != nil {
			log.Fatalln(r)
		}
	}
}
