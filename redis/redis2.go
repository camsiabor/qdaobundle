package redis

/*

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis"
	"github.com/camsiabor/qstock/global"
	"github.com/camsiabor/qstock/util"
	"strconv"
	"strings"
	"time"
)


const DB_DEFAULT string = "def";
const DB_HISTORY string = "history";
const DB_COMMON string = "common";

type  GetDaoManager struct {
	channelHeartbeat chan string;
	rclients map[string]*redis.Client;
}

var _instance * GetDaoManager;

func GetDaoManager() * GetDaoManager {
	if _instance == nil {
		_instance = &GetDaoManager {}
		_instance.channelHeartbeat = make(chan string);
		_instance.rclients = make(map[string]*redis.Client);;
	}
	return _instance
}

// TODO lock
// TODO reconnection
// TODO connection pool

func (o GetDaoManager) Init() {
	var global = global.GetInstance();
	var config_redis = util.GetMap(global.Config,  true, "database", "redis")
	var host_default = util.GetStr(config_redis, "127.0.0.1", "host");
	var port_default = util.GetStr(config_redis, "6379", "port");
	var pass_default = util.GetStr(config_redis, "", "password");
	for id, one := range config_redis {
		var err error;
		var rclient = o.rclients[id];
		if (rclient != nil) {
			_, err = rclient.Ping().Result()
			if (err == nil) {
				continue;
			} else {
				rclient.Close();
			}
		}

		var _, castok = one.(map[string]interface{});
		if (!castok) {
			continue;
		}

		var redis_host = util.GetStr(one,  host_default, "host")
		var redis_port = util.GetStr(one,  port_default, "port")
		var redis_password = util.GetStr(one,  pass_default, "password")
		var redis_db = util.GetInt(one,  0, "db")
		rclient, err = o.Conn(id, redis_host, redis_port, redis_password, redis_db)
		if err == nil {
			o.rclients[id] = rclient;
			qlog.Log(qlog.INFO, "redis", "connected to redis", id, redis_host+":"+redis_port, strconv.Itoa(redis_db))
		} else {
			qlog.Log(qlog.FATAL, "redis", "unable to connect to redis", id, redis_host+":"+redis_port, strconv.Itoa(redis_db))
		}
	}
}

func (o GetDaoManager) Destroy() {
	close(o.channelHeartbeat);
	for _, rclient := range o.rclients {
		if (rclient != nil) {
			rclient.Close();
		}
	}
}

func (o GetDaoManager) run() {
	for {
		 var ok bool = true;
		 var cmd string = "heartbeat";
		 var timeout = time.After(time.Duration(10) * time.Second);
		 select {
		 	case cmd, ok = <-o.channelHeartbeat:
		 		cmd = strings.Trim(cmd, " ");
		 	case <-timeout:
		 		cmd = "heartbeat";
		 }
		 var global = global.GetInstance();
		 if (!ok || !global.Continue || cmd == "close") {
		 	break;
		 }
		 o.Init();
	}
}

func (o GetDaoManager) Conn(id string, ip string, port string, password string, db int) (rclient * redis.Client, xerr error) {

	var err error;
	rclient = o.rclients[id];
	if (rclient != nil) {
		_, err = rclient.Ping().Result()
		if (err == nil) {
			return rclient, nil;
		}
	}
	var options = redis.Options{
		Addr:     ip + ":" + port,
		Password: password,
		DB:       db,
	}
	rclient = redis.NewClient(&options)
	_, err = rclient.Ping().Result()
	if (err != nil) {
		rclient.Close();
	}
	o.rclients[id] = rclient;
	return rclient, err
}

func (o GetDaoManager) Release(id string) {

	var rclient = o.rclients[id];
	if rclient == nil {
		return;
	}

	var err = rclient.Close()
	if err != nil {
		fmt.Println()
	}
	rclient = nil
	qlog.Log(qlog.INFO, "redis", "close")
}

func (o GetDaoManager) Get(id string) (rclient * redis.Client){
	return o.rclients[id];
}

func Redis_hget_map(rclient *redis.Client, key string, field string) (map[string]interface{}, error) {
	var scmd = rclient.HGet(key, field);
	var data, err = scmd.Result();
	if (err != nil) {
		return nil, err;
	}
	var r map[string]interface{};
	err = json.Unmarshal([]byte(data), &r);
	if (err != nil) {
		return nil, err;
	}
	return r, nil;
}

func Redis_hset_map(rclient *redis.Client, key string, field string, value interface{}) (bool, error) {
	var bytes, err = json.Marshal(value);
	if (err != nil) {
		return false, err;
	}
	var cmd = rclient.HSet(key, field, string(bytes[:]));
	var ok, rerr = cmd.Result();
	if (rerr != nil) {
		return false, rerr;
	}
	return ok, nil;
}

*/
