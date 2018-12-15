package qredis

import (
	"encoding/json"
	"fmt"
	"github.com/camsiabor/qcom/qdao"
	"github.com/camsiabor/qcom/qref"
	"github.com/camsiabor/qcom/util"
	"github.com/gomodule/redigo/redis"
	"github.com/pkg/errors"
	"strconv"
	"strings"
	"time"
)

// http://eastfisher.org/2018/03/18/redigo_study/

type DaoRedis struct {
	qdao.Config
	pool *redis.Pool
}

func (o *DaoRedis) Configure(
	name string, daotype string,
	host string, port int, user string, pass string, database string,
	options map[string]interface{}) error {
	return o.Config.Configure(name, daotype, host, port, user, pass, database, options)
}

func (o *DaoRedis) Conn() (interface{}, error) {
	var redisdb, perr = strconv.Atoi(o.Database)
	if perr != nil {
		redisdb = 0
	}
	o.Lock()
	defer o.UnLock()
	if o.pool == nil {
		o.pool = &redis.Pool{
			MaxIdle:     o.MaxIdle,
			IdleTimeout: time.Duration(o.IdleTimeout) * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", o.Host+":"+strconv.Itoa(o.Port),
					redis.DialDatabase(redisdb),
					redis.DialPassword(o.Pass),
					redis.DialKeepAlive(time.Duration(o.KeepAlive)*time.Second),
				)
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		}
	}

	var conn = o.pool.Get()
	var _, err = conn.Do("PING")
	return conn, err
}

func (o *DaoRedis) IsConnected() bool {
	if o.pool == nil {
		return false
	}
	var conn = o.pool.Get()
	if conn == nil {
		return false
	}
	var _, err = conn.Do("PING")
	if err == nil {
		return true
	} else {
		return false
	}
}

func (o *DaoRedis) Close() error {
	if o.pool != nil {
		return o.pool.Close()
	}
	return nil
}

func (o *DaoRedis) Agent() (interface{}, error) {
	if o.pool == nil {
		return nil, errors.New("not init")
	}
	return o.pool.Get(), nil
}

func (o *DaoRedis) SelectDB(db string) error {
	var conn = o.pool.Get()
	var rdb = o.DBMapping[db]
	if rdb == nil {
		rdb = 0
	}
	var _, err = conn.Do("SELECT", rdb)
	return err
}

func (o *DaoRedis) UpdateDB(db string, options interface{}, create bool, override bool) (interface{}, error) {
	o.Lock()
	defer o.UnLock()

	if o.DBMapping[db] != nil {
		return false, fmt.Errorf("db already exist %v", o.DBMapping)
	}

	var index = util.GetInt(options, -1, "index")
	if index < 0 {
		return false, fmt.Errorf("index not found in options %v", options)
	}

	var exist bool
	if !create {
		exist, err := o.ExistDB(db)
		if exist || err != nil {
			return nil, err
		}
	}

	if !override {
		if exist {
			return nil, nil
		}
	}

	if !override {
		for k, v := range o.DBMapping {
			var vindex = util.AsInt(v, -1)
			if vindex == index {
				return false, fmt.Errorf("index already defined %v = %v", k, v)
			}
		}
	}

	o.DBMapping[db] = index
	return true, nil
}

func (o *DaoRedis) UpdateGroup(db string, group string, options interface{}, create bool, override bool) (interface{}, error) {
	panic("implement")
	var conn = o.getConn(db)
	var exist bool
	if !create {
		exist, err := o.ExistGroup(db, group)
		if exist || err != nil {
			return nil, err
		}
	}
	if !override && exist {
		return nil, nil
	}
	conn.Do("HSET", group, "_", "{}")
	conn.Do("HDEL", group, "_")
	return nil, nil
}

func (o *DaoRedis) GetDB(db string) (interface{}, error) {
	panic("implement")
}

func (o *DaoRedis) GetGroup(db string, group string) (interface{}, error) {
	panic("implement")
}

func (o *DaoRedis) ExistDB(db string) (bool, error) {
	var rdb = o.DBMapping[db]
	return rdb != nil, nil
}

func (o *DaoRedis) ExistGroup(db string, group string) (bool, error) {
	var conn = o.getConn(db)
	var r, err = redis.Strings(conn.Do("KEYS", group))
	if err != nil {
		return false, err
	}
	return len(r) > 0, nil
}

func (o *DaoRedis) getConn(db string) redis.Conn {
	var conn = o.pool.Get()
	var rdb = o.DBMapping[db]
	if rdb == nil {
		rdb = 0
	}
	conn.Do("SELECT", rdb)
	return conn
}

func (o *DaoRedis) Get(db string, group string, id string, unmarshal bool) (ret interface{}, err error) {
	var conn = o.getConn(db)
	if len(group) == 0 {
		ret, err = redis.StringMap(conn.Do("HGETALL", id))
		if err == redis.ErrNil {
			ret = nil
			err = nil
		}
	} else {
		var sret string
		sret, err = redis.String(conn.Do("HGET", group, id))
		if err == nil {
			ret = sret
		} else {
			if err == redis.ErrNil {
				err = nil
			}
		}
		if ret != nil && unmarshal {
			if unmarshal {
				var m map[string]interface{}
				err = json.Unmarshal([]byte(sret), &m)
				ret = m
			} else {
				ret = sret
			}
		}
	}
	return ret, err
}

func (o *DaoRedis) Gets(db string, group string, ids []interface{}, unmarshal bool) (rets []interface{}, err error) {
	var retscount = 0
	var conn = o.getConn(db)
	var idslen = len(ids)
	rets = make([]interface{}, idslen)

	var cmd string
	var hget = len(group) > 0
	if hget {
		cmd = "HGET"
	} else {
		cmd = "HGETALL"
	}
	for i := 0; i < idslen; i++ {
		if hget {
			err = conn.Send(cmd, group, ids[i])
		} else {
			err = conn.Send(cmd, ids[i])
		}
		if err != nil {
			return nil, err
		}
	}

	conn.Flush()
	for i := 0; i < idslen; i++ {
		var one, oneerr = conn.Receive()
		if oneerr == redis.ErrNil {
			continue
		}
		if oneerr != nil {
			return nil, oneerr
		}
		if hget {
			var sone, _ = redis.String(one, oneerr)
			if unmarshal {
				var m map[string]interface{}
				oneerr = json.Unmarshal([]byte(sone), &m)
			}
		} else {
			one, _ = redis.StringMap(one, oneerr)
		}
		if oneerr != nil {
			return nil, oneerr
		}
		rets[retscount] = one
	}
	return rets[:retscount], err
}

func (o *DaoRedis) Keys(db string, group string, wildcard string) (keys []string, err error) {
	var conn = o.getConn(db)
	if len(group) == 0 {
		conn.Send("KEYS", wildcard)
	} else {
		conn.Send("HKEYS", group)
	}
	conn.Flush()
	return redis.Strings(conn.Receive())
}

func (o *DaoRedis) Query(db string, query string, args []interface{}) (interface{}, error) {
	panic("implement me")
}

func (o *DaoRedis) Update(db string, group string, id string, val interface{}, override bool, marshal int) (interface{}, error) {
	var conn = o.getConn(db)
	if marshal > 0 {
		bytes, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		val = string(bytes[:])
	}
	if len(group) == 0 {
		if qref.IsMapOrStruct(val) {
			var args = redis.Args{}.Add(id).AddFlat(val)
			return redis.String(conn.Do("HMSET", args...))
		} else {
			if marshal < 0 {
				sval, err := qref.MarshalLazy(val)
				if err != nil {
					return nil, err
				}
				val = sval
			}
			if override {
				return redis.String(conn.Do("SET", id, val))
			} else {
				return redis.Int(conn.Do("SETNX", id, val))
			}
		}
	} else {
		if marshal < 0 {
			sval, err := qref.MarshalLazy(val)
			if err != nil {
				return nil, err
			}
			val = sval
		}
		if override {
			return redis.Int(conn.Do("HSET", group, id, val))
		} else {
			return redis.Int(conn.Do("HSETNX", group, id, val))
		}
	}
	return nil, nil
}

func (o *DaoRedis) Updates(db string, group string, ids []interface{}, vals []interface{}, override bool, marshal int) (interface{}, error) {
	var conn = o.getConn(db)
	var idslen = len(ids)
	var valslen = len(vals)
	if idslen != valslen {
		return nil, fmt.Errorf("ids len != valslen, %d != %d", idslen, valslen)
	}
	var useset = len(group) == 0
	for i := 0; i < idslen; i++ {
		var id = ids[i]
		var val = vals[i]
		if marshal > 0 {
			bytes, err := json.Marshal(val)
			if err != nil {
				return nil, err
			}
			val = string(bytes[:])
		}
		if useset {
			if qref.IsMapOrStruct(val) {
				var args = redis.Args{}.Add(id).AddFlat(val)
				conn.Send("HMSET", args...)
			} else {
				if marshal < 0 {
					sval, err := qref.MarshalLazy(val)
					if err != nil {
						return nil, err
					}
					val = sval
				}
				if override {
					conn.Send("SET", id, val)
				} else {
					conn.Send("SETNX", id, val)
				}
			}
		} else {
			if marshal < 0 {
				sval, err := qref.MarshalLazy(val)
				if err != nil {
					return nil, err
				}
				val = sval
			}
			if override {
				conn.Send("HSET", group, id, val)
			} else {
				conn.Send("HSETNX", group, id, val)
			}
		}
	}
	conn.Flush()
	var err error
	var rets = make([]interface{}, idslen)
	for i := 0; i < idslen; i++ {
		var ret, rerr = conn.Receive()
		if rerr != nil {
			if rerr == redis.ErrNil {
				rerr = nil
			} else {
				err = rerr
			}
		}
		rets[i] = ret
	}
	return rets, err
}

func (o *DaoRedis) Delete(db string, group string, id string) (interface{}, error) {
	var conn = o.getConn(db)
	if len(group) == 0 {
		conn.Send("DEL", id)
	} else {
		conn.Send("HDEL", group, id)
	}
	conn.Flush()
	return redis.Int(conn.Receive())
}

func (o *DaoRedis) Deletes(db string, group string, ids []interface{}) (interface{}, error) {
	var conn = o.getConn(db)
	if len(group) == 0 {
		conn.Send("DEL", ids...)
	} else {
		var args = redis.Args{}.Add(group).AddFlat(ids)
		conn.Send("HDEL", args...)
	}
	conn.Flush()
	return redis.Int(conn.Receive())
}

func (o *DaoRedis) ScanAsMap(db string, group string, from int, size int, unmarshal bool, query ...interface{}) (ret map[string]interface{}, cursor int, total int, err error) {
	var conn = o.getConn(db)
	var cmd string
	if len(group) == 0 {
		cmd = "SCAN"
	} else {
		cmd = "HSCAN"

	}
	if query == nil {
		conn.Send(cmd, from)
	} else {
		conn.Send(cmd, append([]interface{}{from}, query...)...)
	}

	raw, err := conn.Receive()
	if err != nil {
		return nil, 0, 0, err
	}
	bulks, err := redis.Values(raw, err)
	cursor, _ = redis.Int(bulks[0], err)
	mss, _ := redis.StringMap(bulks[1], err)
	m := util.AsMap(mss, false)
	return m, len(m), 0, err
}

func (o *DaoRedis) Scan(db string, group string, from int, size int, unmarshal bool, query ...interface{}) (ret []interface{}, cursor int, total int, err error) {

	//for k, v := range m {
	//
	//}
	//return ret, cursor, len(m), err;
	return nil, 0, 0, nil
}

func (o *DaoRedis) Script(db string, group string, id string, script string, args []interface{}) (interface{}, error) {
	var conn = o.getConn(db)
	var keycount int = 0
	if args != nil {
		keycount = len(args)
	}
	var rscript = redis.NewScript(keycount, script)
	return redis.String(rscript.Do(conn, redis.Args{}.AddFlat(args)...))
}

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
