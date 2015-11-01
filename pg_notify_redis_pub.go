package main

import (
	"encoding/json"
	"fmt"
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
		panic(err)
	}
}

func publish(conn redis.Conn, payload []string) {
	m := make(map[string]string)

	m["id"] = payload[0]
	m["account_id"] = payload[1]
	m["action_type"] = payload[2]
	m["referrer_id"] = payload[3]
	m["amount"] = payload[6]
	m["balance_after"] = payload[7]

	jsonVal, _ := json.Marshal(m)
	msg := string(jsonVal)
	conn.Do("PUBLISH", "balance_"+m["account_id"], msg)
	conn.Do("PUBLISH", m["action_type"]+"_"+m["account_id"], msg)
}

func waitForNotification(dbcluter DBCluster, parition string) {
	conninfo := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=require", "read", dbcluter.Password, dbcluter.Parition[parition].Write.IP, "test")
	listener := pq.NewListener(conninfo, 5*time.Second, 10*time.Second, nil)
	err := listener.Listen("getwork")
	checkErr(err)

	fmt.Println("Listing to", parition)
	redisdb, err := redis.DialURL(os.Getenv("REDIS_URL"))
	checkErr(err)
	var notification *pq.Notification
	for {
		select {
		case notification = <-listener.Notify:
			if notification != nil {
				publish(redisdb, regexp.MustCompile(",").Split(notification.Extra, -1))
			}

		case <-time.After(60 * time.Second):
			fmt.Println("no notifications for 60 seconds...")
		}
	}
}

func main() {
	var dbcluster DBCluster
	source, _ := ioutil.ReadFile("db.yml")
	yaml.Unmarshal(source, &dbcluster)

	fmt.Println(dbcluster)
	for parition, _ := range dbcluster.Parition {
		waitForNotification(dbcluster, parition)
	}

	// Here we just wait for kill signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	s := <-c
	fmt.Println("Got a kill signal:", s)
}
