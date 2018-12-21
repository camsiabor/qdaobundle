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

func (o *DaoRedis) UpdateDB(db string, options interface{}, create bool, override bool, opt qdao.UOpt) (interface{}, error) {
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

func (o *DaoRedis) UpdateGroup(db string, group string, options interface{}, create bool, override bool, opt qdao.UOpt) (interface{}, error) {
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

func (o *DaoRedis) GetDB(db string, opt qdao.QOpt) (interface{}, error) {
	panic("implement")
}

func (o *DaoRedis) GetGroup(db string, group string, opt qdao.QOpt) (interface{}, error) {
	panic("implement")
}

func (o *DaoRedis) Exists(db string, group string, ids []interface{}) (int64, error) {
	var conn = o.getConn(db)
	if len(group) == 0 {
		var count, err = redis.Int(conn.Do("EXISTS", ids...))
		return int64(count), err
	} else {
		for _, id := range ids {
			conn.Send("HEXISTS", group, id)
		}
		var count int
		var err error
		var total int64 = 0
		var n = len(ids)
		for i := 0; i < n; i++ {
			count, err = redis.Int(conn.Receive())
			total = total + int64(count)
		}
		return total, err
	}
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

func (o *DaoRedis) Get(db string, group string, id interface{}, unmarshal int, opt qdao.QOpt) (ret interface{}, err error) {
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
		if ret != nil && unmarshal != 0 {
			if unmarshal != 0 {
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

func (o *DaoRedis) Gets(db string, group string, ids []interface{}, unmarshal int, opt qdao.QOpt) (rets []interface{}, err error) {
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
			if unmarshal != 0 {
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

func (o *DaoRedis) Keys(db string, group string, wildcard string, opt qdao.QOpt) (keys []string, err error) {
	var conn = o.getConn(db)
	if len(group) == 0 {
		conn.Send("KEYS", wildcard)
	} else {
		conn.Send("HKEYS", group)
	}
	conn.Flush()
	return redis.Strings(conn.Receive())
}

func (o *DaoRedis) Query(db string, query string, args []interface{}, opt qdao.QOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoRedis) Update(db string, group string, id interface{}, val interface{}, override bool, marshal int, opt qdao.UOpt) (interface{}, error) {
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

func (o *DaoRedis) Updates(db string, group string, ids []interface{}, vals []interface{}, override bool, marshal int, opt qdao.UOpt) (interface{}, error) {
	var groups = make([]string, len(ids))
	return o.UpdateBatch(db, groups, ids, vals, override, marshal, opt)
}

func (o *DaoRedis) UpdateBatch(db string, groups []string, ids []interface{}, vals []interface{}, override bool, marshal int, opt qdao.UOpt) (interface{}, error) {
	var conn = o.getConn(db)
	var idslen = len(ids)
	var valslen = len(vals)
	if idslen != valslen {
		return nil, fmt.Errorf("ids len != valslen, %d != %d", idslen, valslen)
	}

	for i := 0; i < idslen; i++ {
		var id = ids[i]
		var group = groups[i]
		var val = vals[i]
		var useset = len(group) == 0
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

func (o *DaoRedis) Delete(db string, group string, id interface{}, opt qdao.DOpt) (interface{}, error) {
	var conn = o.getConn(db)
	if len(group) == 0 {
		conn.Send("DEL", id)
	} else {
		conn.Send("HDEL", group, id)
	}
	conn.Flush()
	return redis.Int(conn.Receive())
}

func (o *DaoRedis) Deletes(db string, group string, ids []interface{}, opt qdao.DOpt) (interface{}, error) {
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

func (o *DaoRedis) ScanAsMap(db string, group string, from int, size int, unmarshal int, query ...interface{}) (ret map[string]interface{}, cursor int, total int, err error) {
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

func (o *DaoRedis) Scan(db string, group string, from int, size int, unmarshal int, query ...interface{}) (ret []interface{}, cursor int, total int, err error) {

	//for k, v := range m {
	//
	//}
	//return ret, cursor, len(m), err;
	return nil, 0, 0, nil
}

func (o *DaoRedis) Script(db string, group string, id interface{}, script string, args []interface{}, opt qdao.QOpt) (interface{}, error) {
	var conn = o.getConn(db)
	var keycount int = 0
	if args != nil {
		keycount = len(args)
	}
	var rscript = redis.NewScript(keycount, script)
	return redis.String(rscript.Do(conn, redis.Args{}.AddFlat(args)...))
}
