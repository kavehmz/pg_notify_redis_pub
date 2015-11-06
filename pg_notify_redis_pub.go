package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"regexp"
	"time"

	"io/ioutil"

	"github.com/garyburd/redigo/redis"
	"github.com/lib/pq"
	"gopkg.in/yaml.v2"
)

type DBCluster struct {
	Password string
	Parition map[string]struct {
		Write struct {
			Name string
			IP   string
		}
	} `yaml:",inline"`
}

func checkErr(err error) {
	if err != nil {
		log.Println(err)
	}
}

func redisconn(redisdb *redis.Conn) {
	if *redisdb == nil {
		conn, err := redis.DialURL(os.Getenv("REDIS_URL"))
		checkErr(err)
		*redisdb = conn
	}
	_, err := (*redisdb).Do("PING")
	if err != nil {
		conn, err := redis.DialURL(os.Getenv("REDIS_URL"))
		checkErr(err)
		*redisdb = conn
	}
}

func publish(redisdb redis.Conn, payload []string) {
	m := make(map[string]string)

	for k, v := range []string{"id", "account_id", "action_type", "referrer_type", "contract_id", "payment_id", "amount", "balance_after"} {
		m[v] = payload[k]
	}

	jsonVal, _ := json.Marshal(m)
	msg := string(jsonVal)

	fmt.Println("Pushlishing ...")
	redisdb.Do("PUBLISH", "balance_"+m["account_id"], msg)
	redisdb.Do("PUBLISH", m["action_type"]+"_"+m["account_id"], msg)
}

func waitForNotification(dbcluter DBCluster, parition string) {
	conninfo := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=require", "read", dbcluter.Password, dbcluter.Parition[parition].Write.IP, "regentmarkets")
	listener := pq.NewListener(conninfo, 5*time.Second, 10*time.Second, nil)
	db, _ := sql.Open("postgres", conninfo)
	err := db.Ping()
	if err != nil {
		log.Fatal(err)
	}
	err = listener.Listen("transactions_watcher")
	checkErr(err)
	fmt.Println("Listing to", parition)

	var redisdb redis.Conn
	var notification *pq.Notification
	for {
		select {
		case notification = <-listener.Notify:
			if notification != nil {
				redisconn(&redisdb)
				publish(redisdb, regexp.MustCompile(",").Split(notification.Extra, -1))
			}

		case <-time.After(60 * time.Second):
			fmt.Println("no notifications for 60 seconds...")
		}
	}
}

func main() {
	var dbcluster DBCluster
	source, err := ioutil.ReadFile("db.yml")
	checkErr(err)
	yaml.Unmarshal(source, &dbcluster)

	for parition, _ := range dbcluster.Parition {
		go waitForNotification(dbcluster, parition)
	}

	// Here we just wait for kill signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	fmt.Println("Got a kill signal:", s)
}
