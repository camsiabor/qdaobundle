package qelastic

import (
	"github.com/camsiabor/qcom/qdao"
	"github.com/camsiabor/qcom/qlog"
	"github.com/camsiabor/qcom/util"
	"github.com/olivere/elastic"
	"github.com/pkg/errors"
	"strconv"
)

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

func (o *DaoElastic) Agent() (agent interface{}, err error) {
	return o.client, nil
}

func (o *DaoElastic) SelectDB(db string) error {
	return nil
}

func (o *DaoElastic) UpdateDB(db string, options interface{}, create bool, override bool) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) UpdateGroup(db string, group string, options interface{}, create bool, override bool) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) ExistDB(db string) (bool, error) {
	panic("implement me")
}

func (o *DaoElastic) ExistGroup(db string, group string) (bool, error) {
	panic("implement me")
}

func (o *DaoElastic) GetDB(db string) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) GetGroup(db string, group string) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Keys(db string, group string, wildcard string) (keys []string, err error) {
	//var dsl = "{\"_source\":[\"\"]\"query\":{\"wildcard\":{\"value\":\"%s\"} } }";
	//dsl = fmt.Sprint(dsl, wildcard);
	//var esindex = o.getIndexName(db, group);
	//resp, err := o.indexService(db, group).BodyString(dsl).Do(context.Background());
	//if (err != nil) {
	//	return nil, err;
	//}
	//resp.Result
	//return nil, err;
	panic("implement")
}

func (o *DaoElastic) Get(db string, group string, id string, unmarshal bool) (ret interface{}, err error) {
	panic("implement me")
}

func (o *DaoElastic) Gets(db string, group string, ids []interface{}, unmarshal bool) (rets []interface{}, err error) {
	panic("implement me")
}

func (o *DaoElastic) Query(db string, query string, args []interface{}) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Scan(db string, group string, from int, size int, unmarshal bool, query ...interface{}) (ret []interface{}, cursor int, total int, err error) {
	panic("implement me")
}

func (o *DaoElastic) ScanAsMap(db string, group string, from int, size int, unmarshal bool, query ...interface{}) (ret map[string]interface{}, cursor int, total int, err error) {
	panic("implement me")
}

func (o *DaoElastic) Update(db string, group string, id string, val interface{}, override bool, marshal int) (interface{}, error) {
	//o.client.Index().Index(db)
	panic("imple")
}

func (o *DaoElastic) Updates(db string, group string, ids []interface{}, vals []interface{}, override bool, marshal int) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Delete(db string, group string, id string) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Deletes(db string, group string, ids []interface{}) (interface{}, error) {
	panic("implement me")
}

func (o *DaoElastic) Script(db string, group string, id string, script string, args []interface{}) (interface{}, error) {
	panic("implement me")
}
