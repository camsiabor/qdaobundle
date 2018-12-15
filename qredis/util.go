package qredis

import (
	"encoding/json"
	"fmt"
	"github.com/camsiabor/qcom/qref"
	"github.com/gomodule/redigo/redis"
	"strings"
)

/* ============================ supplement ========================== */

func RHGetMap(rclient redis.Conn, key string, field string) (map[string]interface{}, error) {
	var data, err = redis.String(rclient.Do("HGET", key, field))
	if err != nil {
		return nil, err
	}
	var r map[string]interface{}
	err = json.Unmarshal([]byte(data), &r)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func RHSetMap(rclient redis.Conn, key string, field string, value interface{}) (bool, error) {
	var bytes, err = json.Marshal(value)
	if err != nil {
		return false, err
	}
	var ok, rerr = redis.Bool(rclient.Do("HSET", key, field, string(bytes[:])))
	if rerr != nil {
		return false, rerr
	}
	return ok, nil
}

func RDel(rclient redis.Conn, key string) {
	rclient.Do("DEL", key)
}

func RHMSet(rclient redis.Conn, key string, value interface{}) (bool, error) {
	var args = redis.Args{}.Add(key).AddFlat(value)
	var r, err = redis.Bool(rclient.Do("HMSET", args...))
	return r, err
}

func RCmd(rclient redis.Conn, cmd string, args ...interface{}) (interface{}, error) {
	rawreply, err := rclient.Do(cmd, args...)
	if err != nil {
		return nil, err
	}
	return RParse(cmd, rawreply, err)
}

func RParseScan(val interface{}, err error) (interface{}, error) {
	if err != nil {
		return val, err
	}
	var rets, _ = redis.Values(val, err)
	rets[0], _ = redis.Int(rets[0], err)
	rets[1], _ = redis.Strings(rets[1], err)
	return rets, nil
}

var _rparsers map[string]interface{}

func RParse(cmd string, rawreply interface{}, err error) (interface{}, error) {

	cmd = strings.ToUpper(cmd)
	if _rparsers == nil {
		_rparsers = make(map[string]interface{})
		_rparsers["DEL"] = redis.Bool
		_rparsers["DUMP"] = redis.Bytes
		_rparsers["RESTORE"] = redis.String
		_rparsers["EXISTS"] = redis.Int64
		_rparsers["EXPIRE"] = redis.Int64
		_rparsers["EXPIREAT"] = redis.Int64
		_rparsers["PEXPIRE "] = redis.Int64
		_rparsers["PEXPIREAT"] = redis.Int64
		_rparsers["TTL"] = redis.Int64
		_rparsers["PTTL"] = redis.Int64
		_rparsers["GET"] = redis.String
		_rparsers["KEYS"] = redis.Strings
		_rparsers["PERSIST"] = redis.Int64
		_rparsers["RENAME"] = redis.String
		_rparsers["SORT"] = redis.Strings

		_rparsers["STRLEN"] = redis.Int64

		_rparsers["HDEL"] = redis.Int64
		_rparsers["HSET"] = redis.Int64
		_rparsers["HSETNX"] = redis.Int64
		_rparsers["HGET"] = redis.String
		_rparsers["HKEYS"] = redis.Strings
		_rparsers["HVALS"] = redis.Strings
		_rparsers["HMSET"] = redis.String
		_rparsers["HMGET"] = redis.Strings
		_rparsers["HEXISTS"] = redis.Int64
		_rparsers["HGETALL"] = redis.StringMap

		_rparsers["LPUSH"] = redis.Int64
		_rparsers["LPOP"] = redis.Int64
		_rparsers["LLEN"] = redis.Int64
		_rparsers["LSET"] = redis.String
		_rparsers["LRANGE"] = redis.Strings

		_rparsers["INCR"] = redis.Int64
		_rparsers["INCRBY"] = redis.Int64
		_rparsers["INCRBYFLOAT"] = redis.Float64

		_rparsers["SADD"] = redis.Int64
		_rparsers["SREM"] = redis.Int64

		_rparsers["ZADD"] = redis.Int64
		_rparsers["ZREM"] = redis.Int64
		_rparsers["ZRANGE"] = redis.Strings

		_rparsers["SCAN"] = RParseScan
		_rparsers["HSCAN"] = RParseScan
		_rparsers["SSCAN"] = RParseScan
		_rparsers["ZSCAN"] = RParseScan

		_rparsers["SELECT"] = redis.String

	}

	var parser = _rparsers[cmd]
	if parser != nil {
		//var orierr error;
		//if (err == nil) {
		//	err = fmt.Errorf("");
		//} else {
		//	orierr = err;
		//}
		var rets = qref.FuncCall(parser, rawreply, err)
		var parsedreply = rets[0].Interface()
		return parsedreply, err
	}

	return nil, fmt.Errorf("cmd %s not support yet", cmd)
}
