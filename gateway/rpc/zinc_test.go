package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	zinc "github.com/zinclabs/sdk-go-zincsearch"
)

const zincUrl = "http://localhost:4080"
const port = "1234"
const username = "admin"
const password = "User#123"

const index = FileIndex

func initTestService() Service {
	configuration := zinc.NewConfiguration()
	configuration.Servers = zinc.ServerConfigurations{
		zinc.ServerConfiguration{
			URL: zincUrl,
		},
	}
	apiClient := zinc.NewAPIClient(configuration)
	return Service{
		port:      port,
		zincUrl:   zincUrl,
		username:  username,
		password:  password,
		apiClient: apiClient,
	}
}

//func TestQueryFile(t *testing.T) {
//	service := initTestService()
//	content, err := service.GetContentByDocId(FileIndex, "8c1ae2c7-33df-455a-870a-a41ee25dbcc0")
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println(content)
//}

//func TestQueryPath(t *testing.T) {
//	service := initTestService()
//	res, err := service.ZincQueryByPath(FileIndex, "")
//	if err != nil {
//		panic(err)
//	}
//	fmt.Printf("%v", res)
//	doc, err := GetFileQueryResult(res)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Printf("%v", doc)
//}

//func TestSetupIndex(t *testing.T) {
//	service := initTestService()
//	err := service.setupIndex()
//	if err != nil {
//		panic(err)
//	}
//}

func TestListIndex(t *testing.T) {
	// indexName := "hightlight"
	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
		UserName: username,
		Password: password,
	})

	configuration := zinc.NewConfiguration()
	configuration.Servers = zinc.ServerConfigurations{
		zinc.ServerConfiguration{
			URL: zincUrl,
		},
	}

	apiClient := zinc.NewAPIClient(configuration)
	resp, r, err := apiClient.Index.IndexNameList(ctx).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Index.Exists``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `Exists`: map[string]interface{}
	fmt.Fprintf(os.Stdout, "Response from `Index.Exists`: %v\n", resp)
}

func TestClientSearchV1(t *testing.T) { // string | Index
	query := *zinc.NewV1ZincQuery() // V1ZincQuery | Query
	query.SetSearchType("match")
	params := *zinc.NewV1QueryParams()
	params.SetTerm("123abc")
	query.SetQuery(params)
	query.SetSortFields([]string{"-@timestamp"})
	query.SetMaxResults(5)
	query.SetSource([]string{"content", "filename", "filepath"})

	ctx := context.WithValue(context.TODO(), zinc.ContextBasicAuth, zinc.BasicAuth{
		UserName: username,
		Password: password,
	})

	configuration := zinc.NewConfiguration()
	configuration.Servers = zinc.ServerConfigurations{
		zinc.ServerConfiguration{
			URL: zincUrl,
		},
	}
	apiClient := zinc.NewAPIClient(configuration)
	resp, r, err := apiClient.Search.SearchV1(ctx, index).Query(query).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `SearchApi.SearchV1``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `SearchV1`: V1SearchResponse
	fmt.Fprintf(os.Stdout, "Response from `SearchApi.SearchV1`: %v\n", resp)
	for _, hit := range resp.Hits.Hits {
		fmt.Printf("%v %v\n", hit.GetTimestamp(), hit.GetSource())
	}
}

func TestClientSearchV2(t *testing.T) {
	indexName := "hightlight"
	// eg:
	// {
	// 	"query": {
	// 		"bool": {
	// 			"should": [
	// 				{
	// 					"match": {
	// 						"name": {
	// 							"query": "John"
	// 						}
	// 					}
	// 				}
	// 			]
	// 		}
	// 	}
	// }
	query := *zinc.NewMetaZincQuery() // V1ZincQuery | Query
	highlight := zinc.NewMetaHighlight()
	highlightContent := zinc.NewMetaHighlight()
	fragmentSize := int32(20)
	highlightContent.FragmentSize = &fragmentSize
	highlight.SetFields(map[string]zinc.MetaHighlight{"content": *highlightContent})
	query.SetHighlight(*highlight)

	matchQuery := *zinc.NewMetaMatchQuery()
	matchQuery.SetQuery("身份证号")
	subQuery := *zinc.NewMetaQuery()
	subQuery.SetMatch(map[string]zinc.MetaMatchQuery{
		"content": matchQuery,
	})
	boolQuery := *zinc.NewMetaBoolQuery()
	boolQuery.SetShould([]zinc.MetaQuery{subQuery})
	queryQuery := *zinc.NewMetaQuery()
	queryQuery.SetBool(boolQuery)
	query.SetQuery(queryQuery)

	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
		UserName: username,
		Password: password,
	})

	configuration := zinc.NewConfiguration()
	configuration.Servers = zinc.ServerConfigurations{
		zinc.ServerConfiguration{
			URL: zincUrl,
		},
	}

	apiClient := zinc.NewAPIClient(configuration)
	resp, r, err := apiClient.Search.Search(ctx, indexName).Query(query).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `SearchApi.Search``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	for _, hit := range resp.Hits.Hits {
		for _, highlightRes := range hit.Highlight {
			// fmt.Printf("hightlight %v\n", highlightRes)
			for _, hh := range highlightRes.([]interface{}) {
				fmt.Println(reflect.TypeOf(hh).String())
				fmt.Println(hh.(string))
			}

		}
	}
}

func TestClientInput(t *testing.T) {
	// string | Index
	id := uuid.NewString() // string | ID
	document := map[string]interface{}{
		"filename": "test.txt",
		"filepath": "/usr/local/test.txt",
		"content":  "你好 hello 再见 goodbye",
	} // map[string]interface{} | Document

	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
		UserName: "admin",
		Password: "User#123",
	})
	configuration := zinc.NewConfiguration()
	configuration.Servers = zinc.ServerConfigurations{
		zinc.ServerConfiguration{
			URL: "http://localhost:4080",
		},
	}

	apiClient := zinc.NewAPIClient(configuration)
	resp, r, err := apiClient.Document.IndexWithID(ctx, index, id).Document(document).Execute()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error when calling `Document.IndexWithID``: %v\n", err)
		fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
	}
	// response from `IndexWithID`: MetaHTTPResponseID
	fmt.Fprintf(os.Stdout, "Response from `Document.IndexWithID`: %v\n", resp.GetId())
}

//func TestDelete(t *testing.T) {
//	docId := "id_example123"
//	res, err := RpcServer.ZincDelete(docId, index)
//	if err != nil {
//		t.Fatal(err)
//	}
//	fmt.Println(string(res))
//}

func TestQuery(t *testing.T) {
	url := zincUrl + "/api/_analyze"
	req, err := http.NewRequest("POST", url, strings.NewReader(`{
		"analyzer" : "keyword",
		"text" : ""
	  }`))
	if err != nil {
		panic(err)
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		panic(err)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

func TestFormatFilename(t *testing.T) {
	fmt.Println(FormatFilename("NihaoTest_something.txt"))
}

func TestJsonRss(t *testing.T) {
	rssInput := RssInputRequest{
		RssMeta: RssMeta{
			Name:    "nihao",
			EntryId: 123,
			Created: time.Now().Unix(),
			FeedInfos: []FeedInfo{{
				FeedId:   1,
				FeedName: "bbc news",
				FeedIcon: "http://bbc.com",
			}},
			Borders: []Border{{
				Name: "nihao",
				Id:   0,
			}},
		},
		Content: "this is a rainy day today, everything is going well",
	}
	b, err := json.Marshal(&rssInput)
	if err != nil {
		panic(err)
	}
	fmt.Println(string(b))
}
