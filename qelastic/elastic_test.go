package qelastic

import (
	"context"
	"fmt"
	"github.com/olivere/elastic"
	"testing"
	"time"
)

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
