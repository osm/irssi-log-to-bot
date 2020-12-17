package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"time"
	"unicode/utf8"

	_ "github.com/lib/pq"
)

var logLineRegexp = regexp.MustCompile("^([0-9][0-9]:[0-9][0-9]) (<[^>]*>) (.*)$")
var nickReplaceRegexp = regexp.MustCompile("[@+ <>]")

func die(err error) {
	fmt.Println(err)
	os.Exit(1)
}

func newUUID() string {
	uuid := make([]byte, 16)
	io.ReadFull(rand.Reader, uuid)
	uuid[8] = uuid[8]&^0xc0 | 0x80
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:])
}

func trim(s string) string {
	r := s
	if !utf8.ValidString(s) {
		v := make([]rune, 0, len(s))
		for i, r := range s {
			if r == utf8.RuneError {
				_, size := utf8.DecodeRuneInString(s[i:])
				if size == 1 {
					continue
				}
			}
			v = append(v, r)
		}
		r = string(v)
	}
	return r
}

func main() {
	date := flag.String("date", "", "date")
	cs := flag.String("cs", "", "postgres connection string")
	flag.Parse()

	if *date == "" {
		fmt.Println("-date is required")
		os.Exit(1)
	}
	if *cs == "" {
		fmt.Println("-cs is required")
		os.Exit(1)
	}

	var err error
	var db *sql.DB
	if db, err = sql.Open("postgres", *cs); err != nil {
		die(err)
	}

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		die(err)
	}
	stmt, err := tx.Prepare("INSERT INTO log (id, timestamp, nick, message) VALUES($1, $2, $3, $4);")
	if err != nil {
		die(err)
	}

	s := bufio.NewScanner(os.Stdin)
	times := make(map[string]time.Time)
	for s.Scan() {
		txt := s.Text()

		parts := logLineRegexp.FindStringSubmatch(txt)
		if len(parts) == 0 {
			continue
		}

		logTime := parts[1]
		if _, ok := times[logTime]; !ok {
			t, _ := time.Parse("2006-01-02 15:04", fmt.Sprintf("%s %s", *date, logTime))
			times[logTime] = t
		}
		t := times[logTime].Add(time.Millisecond)
		timestamp := t.Format("2006-01-02T15:04:05.999")
		times[logTime] = t

		nick := nickReplaceRegexp.ReplaceAllString(parts[2], "")
		message := trim(parts[3])

		_, err = stmt.Exec(newUUID(), timestamp, nick, message)
		if err != nil {
			die(err)
		}
	}

	err = tx.Commit()
	if err != nil {
		die(err)
	}
}
