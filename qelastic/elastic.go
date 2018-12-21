package qelastic

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/camsiabor/qcom/qdao"
	"github.com/camsiabor/qcom/qlog"
	"github.com/camsiabor/qcom/util"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"strconv"
)

const DEFAULT_ID = "_id"
const DEFAULT_TYPE = "_doc"

type DaoElastic struct {
	qdao.Config
	client *elastic.Client
}

func (o *DaoElastic) Configure(
	name string, daotype string,
	host string, port int, user string, pass string, database string,
	options map[string]interface{}) error {
	return o.Config.Configure(name, daotype, host, port, user, pass, database, options)
}

func (o *DaoElastic) Conn() (agent interface{}, err error) {
	o.Lock()
	defer o.UnLock()
	if o.client != nil {
		return o.client, nil
	}
	var settings = make([]elastic.ClientOptionFunc, 1)
	var url = "http://" + o.Config.Host + ":" + strconv.Itoa(o.Config.Port)
	settings[0] = elastic.SetURL(url)

	if len(o.User) > 0 && len(o.Pass) > 0 {
		settings = append(settings, elastic.SetBasicAuth(o.User, o.Pass))
	}
	// TODO log
	var logger = qlog.GetLogManager().GetDef()
	settings = append(settings, elastic.SetInfoLog(logger))
	settings = append(settings, elastic.SetErrorLog(logger))
	settings = append(settings, elastic.SetTraceLog(logger))

	var gzip = util.GetBool(o.Options, false, "gzip")
	settings = append(settings, elastic.SetGzip(gzip))

	o.client, err = elastic.NewClient(settings...)

	return o.client, err
}

func (o *DaoElastic) Close() error {
	o.Lock()
	defer o.UnLock()
	if o.client == nil {
		return errors.New("not open yet")
	}
	o.client = nil
	return nil
}

func (o *DaoElastic) IsConnected() bool {
	if o.client == nil {
		return false
	}
	return o.client.IsRunning()
}

func (o *DaoElastic) Agent() (agent interface{}, err error) {
	return o.client, nil
}

func (o *DaoElastic) getIndexName(db string, group string) string {
	var mdb = o.DBMapping[db]
	if mdb != nil {
		db = mdb.(string)
	}
	if len(db) == 0 {
		return group
	}
	if len(group) == 0 {
		return db
	}
	return db + "_" + group
}

func (o *DaoElastic) Keys(db string, group string, wildcard string, opt qdao.QOpt) (keys []string, err error) {
	var key = o.Framework.GetGroupKey(db, group, "")
	if len(key) == 0 {
		panic(fmt.Errorf("no key specify in schema %s.%s", db, group))
	}
	var dsl = `{
		"_source" : [ "%s" ],
		"query": { 	
			"wildcard": {
				"name": {
					"%s": "%s"
				}
			}
		}
	}`
	dsl = fmt.Sprintf(dsl, key, key, wildcard)
	esindex := o.getIndexName(db, group)
	resp, err := o.client.Search(esindex).Source(dsl).Do(context.Background())
	if err != nil {
		return nil, err
	}
	if resp.Hits.TotalHits == 0 {
		return []string{}, nil
	}
	var ids = make([]string, resp.Hits.TotalHits)
	for i, hit := range resp.Hits.Hits {
		if key == "_id" {
			ids[i] = hit.Id
		} else {
			var _source map[string]interface{}
			if err := json.Unmarshal(*hit.Source, &_source); err != nil {
				return nil, err
			}
			ids[i] = util.AsStr(_source[key], "")
		}
	}
	return ids, err

}

func (o *DaoElastic) Exists(db string, group string, ids []interface{}) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	var idstr = util.SliceJoin(ids, "\"", "\",\"", "\"")
	var dsl = `{
		"_source" : [ "_id" ],
		"query": {
			"ids" : {
				"type" : "_doc",
					"values" : %s
			}
		}
	}`
	dsl = fmt.Sprintf(dsl, idstr)
	esindex := o.getIndexName(db, group)
	resp, err := o.client.Search(esindex).Source(dsl).Do(context.Background())
	if err != nil {
		return -1, err
	}
	return resp.Hits.TotalHits, err
}

func (o *DaoElastic) Get(db string, group string, id interface{}, unmarshal int, opt qdao.QOpt) (ret interface{}, err error) {
	var esindex = o.getIndexName(db, group)
	var dsl = `{	
    	"query" : {
        	"match_phrase": {
           		"_id": "%s"
			}
    	}
	}`
	dsl = fmt.Sprintf(dsl, id)
	resp, err := o.client.Search(esindex).Source(dsl).Do(context.Background())
	if err != nil {
		return nil, err
	}
	if resp.Hits.TotalHits == 0 {
		return nil, err
	}
	if unmarshal == 0 {
		var jstr = string(*resp.Hits.Hits[0].Source)
		return jstr, nil
	} else {
		var m map[string]interface{}
		var err = json.Unmarshal(*resp.Hits.Hits[0].Source, &m)
		return m, err
	}
}

func (o *DaoElastic) Gets(db string, group string, ids []interface{}, unmarshal int, opt qdao.QOpt) (rets []interface{}, err error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var idstr = util.SliceJoin(ids, "\"", "\",\"", "\"")
	var dsl = `{
		"query": {
			"ids" : {
				"type" : "_doc",
					"values" : %s 
			}
		}
	}`
	dsl = fmt.Sprintf(dsl, idstr)
	esindex := o.getIndexName(db, group)
	_, err = o.client.Search(esindex).Source(dsl).Do(context.Background())
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (o *DaoElastic) Update(db string, group string, id interface{}, val interface{}, override bool, marshal int, opt qdao.UOpt) (interface{}, error) {
	//var esindex = o.getIndexName(db, group)
	//if (!override) {
	//	//o.client.Search(esindex).Query(elastic.NewTermsQuery())
	//}

	//var updateService = o.client.Update().Index(esindex).Type(DEFAULT_TYPE).Id(id).Doc(val)
	//updateService.DocAsUpsert(true);
	//var resp, err = updateService.Do(context.Background())
	//if err != nil {
	//	return nil, err
	//}

	panic("implement me")
}

func (o *DaoElastic) Updates(db string, group string, ids []interface{}, vals []interface{}, override bool, marshal int, opt qdao.UOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) UpdateBatch(db string, groups []string, ids []interface{}, vals []interface{}, override bool, marshal int, opt qdao.UOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Delete(db string, group string, id interface{}, opt qdao.DOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Deletes(db string, group string, ids []interface{}, opt qdao.DOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) SelectDB(db string) error {
	return nil
}

func (o *DaoElastic) UpdateDB(db string, options interface{}, create bool, override bool, opt qdao.UOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) UpdateGroup(db string, group string, options interface{}, create bool, override bool, opt qdao.UOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) ExistDB(db string) (bool, error) {
	panic("implement me")
}

func (o *DaoElastic) ExistGroup(db string, group string) (bool, error) {
	panic("implement me")
}

func (o *DaoElastic) GetDB(db string, opt qdao.QOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) GetGroup(db string, group string, opt qdao.QOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Query(db string, query string, args []interface{}, opt qdao.QOpt) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Scan(db string, group string, from int, size int, unmarshal int, opt qdao.QOpt, query ...interface{}) (ret []interface{}, cursor int, total int, err error) {
	panic("implement me")
}

func (o *DaoElastic) ScanAsMap(db string, group string, from int, size int, unmarshal int, opt qdao.QOpt, query ...interface{}) (ret map[string]interface{}, cursor int, total int, err error) {
	panic("implement me")
}

func (o *DaoElastic) Script(db string, group string, id interface{}, script string, args []interface{}, opt qdao.QOpt) (interface{}, error) {
	panic("implement me")
}
