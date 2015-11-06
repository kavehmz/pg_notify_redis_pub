/*
This one is just to have a comparision to anothre bare minimum code.
*/
package main

import (
	"encoding/json"
	"fmt"
	"os"
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

func publish(redisdb redis.Conn, payload []string) {
	m := make(map[string]string)

	for k, v := range []string{"id", "account_id", "action_type", "referrer_type", "contract_id", "payment_id", "amount", "balance_after"} {
		m[v] = payload[k]
	}

	jsonVal, _ := json.Marshal(m)
	msg := string(jsonVal)

	redisdb.Do("PUBLISH", "balance_"+m["account_id"], msg)
	redisdb.Do("PUBLISH", m["action_type"]+"_"+m["account_id"], msg)
}

func waitForNotification(dbcluter DBCluster, parition string) {
	conninfo := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=require", "read", dbcluter.Password, dbcluter.Parition[parition].Write.IP, "regentmarkets")
	listener := pq.NewListener(conninfo, 5*time.Second, 10*time.Second, nil)
	_ = listener.Listen("transactions_watcher")

	redisdb, _ := redis.DialURL(os.Getenv("REDIS_URL"))
	var notification *pq.Notification
	for {
		select {
		case notification = <-listener.Notify:
			if notification != nil {
				publish(redisdb, regexp.MustCompile(",").Split(notification.Extra, -1))
			}
		}
	}
}

func main() {
	var dbcluster DBCluster
	source, _ := ioutil.ReadFile("db.yml")
	yaml.Unmarshal(source, &dbcluster)

	for parition, _ := range dbcluster.Parition {
		go waitForNotification(dbcluster, parition)
	}

	select {}
}
