package qelastic

import (
	"context"
	"fmt"
	"github.com/camsiabor/qcom/qref"
	"github.com/olivere/elastic"
	"testing"
	"time"
)

type MyThing struct {
}

type QMap map[string]interface{}

func (o QMap) Print() {
	o["xxx"] = "ooo"
	fmt.Println(o)
}

func (o *MyThing) Do(m QMap) {
	if m == nil {
		fmt.Println("NULL!")
		return
	}
	m["ada"] = 2
	m["bolin"] = 3
	fmt.Println(m)
}

func (o *MyThing) DoSlice(s []string) {
	fmt.Println(s)
}

func TestMap(t *testing.T) {
	var o *MyThing
	qref.FuncCallByName(o, "DoSlice", []interface{}{"power", "over", "whelming"})
}

func TestElastic(t *testing.T) {

	client, err := elastic.NewClient()
	if err != nil {
		panic(err)
	}

	//_, err = client.CreateIndex("common").Do(context.Background())
	//if (err != nil) {
	//	fmt.Println(err.Error());
	//}

	_, err = client.Index().Index("common").Type("user").Id("0").BodyJson(map[string]interface{}{
		"name": "camsi",
	}).Do(context.Background())
	if err != nil {
		panic(err)
	}

	r, err := client.Search("common").
		Query(elastic.NewTermQuery("name", "camsi")).Pretty(true).Do(context.Background())
	if err != nil {
		panic(err)
	}

	for _, hit := range r.Hits.Hits {
		var s = string(*hit.Source)
		fmt.Println(s)
	}

	time.Sleep(time.Second)

}
