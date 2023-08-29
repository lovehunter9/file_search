// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package rpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/google/uuid"
	//zinc "github.com/zinclabs/sdk-go-zincsearch"
	"io/ioutil"
	"os"
	"reflect"
	"time"
	"wzinc/parser"

	//"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/indices/create"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	"github.com/rs/zerolog/log"
	builtinlog "log"
	"strings"
)

const ContentFieldName = "content"

var ErrQuery = errors.New("query err")

type RssQueryResult struct {
	Index       string   `json:"index"`
	Name        string   `json:"name"`
	DocId       string   `json:"docId"`
	Created     int64    `json:"created"`
	Content     string   `json:"content"`
	Meta        string   `json:"meta"`
	HightLights []string `json:"highlight"`
}

type FileQueryResult struct {
	Index       string   `json:"index"`
	Where       string   `json:"where"`
	Md5         string   `json:"md5"`
	Name        string   `json:"name"`
	DocId       string   `json:"docId"`
	Created     int64    `json:"created"`
	Updated     int64    `json:"updated"`
	Content     string   `json:"content"`
	Type        string   `json:"type"`
	Size        int64    `json:"size"`
	Modified    int64    `json:"modified"`
	HightLights []string `json:"highlight"`
}

func InitES(url string, username string, password string) (es *elasticsearch.TypedClient, err error) {
	builtinlog.SetFlags(0)

	// This example demonstrates how to configure the client's Transport.
	//
	// NOTE: These values are for illustrative purposes only, and not suitable
	//       for any production use. The default transport is sufficient.
	//
	if url == "" {
		url = "http://localhost:4080"
	}
	if username == "" {
		username = "admin"
	}
	if password == "" {
		password = "User#123"
	}

	cfg := elasticsearch.Config{
		Addresses: []string{url + "/es/"},
		Username:  username,
		Password:  password,
		//Transport: &http.Transport{
		//	MaxIdleConnsPerHost:   10,
		//	ResponseHeaderTimeout: time.Millisecond,
		//	DialContext:           (&net.Dialer{Timeout: time.Nanosecond}).DialContext,
		//	TLSClientConfig: &tls.Config{
		//		MinVersion: tls.VersionTLS12,
		//		// ...
		//	},
		//},
	}

	es, err = elasticsearch.NewTypedClient(cfg)
	if err != nil {
		builtinlog.Printf("Error creating the client: %s", err)
	} else {
		builtinlog.Println(es.Info())
		// => dial tcp: i/o timeout
	}
	return
}

func (s *Service) EsSetupIndex() error {

	//"content": {
	//	"type": "text",
	//		"index": true,
	//		"store": true,
	//		"sortable": false,
	//		"aggregatable": false,
	//		"highlightable": true
	//},
	//"md5": {
	//	"type": "text",
	//		"analyzer": "keyword",
	//		"index": true,
	//		"store": false,
	//		"sortable": false,
	//		"aggregatable": false,
	//		"highlightable": false
	//},
	//"where": {
	//	"type": "text",
	//		"analyzer": "keyword",
	//		"index": true,
	//		"store": false,
	//		"sortable": false,
	//		"aggregatable": false,
	//		"highlightable": false
	//}

	expectIndexList := []string{RssIndex, FileIndex}

	// 创建映射定义
	mapping := `
	{
	    "content": {
          "type": "text",
		  "index": true,
          "store": true,
          "sortable": false,
          "aggregatable": false,
          "highlightable": true
	    },
	    "md5": {
          "type": "text",
		  "analyzer": "keyword",
          "index": true,
          "store": false,
          "sortable": false,
          "aggregatable": false,
          "highlightable": false
	    },
	    "where": {
          "type": "text",
		  "analyzer": "keyword",
	      "index": true,
          "store": false,
          "sortable": false,
          "aggregatable": false,
          "highlightable": false
	    }
	}
	`
	var propertiesMap map[string]types.Property
	//{
	//	ContentFieldName: types.TextProperty{},
	//	"md5":            types.KeywordProperty{},
	//	"where":          types.KeywordProperty{},
	//}
	fmt.Println(propertiesMap)
	err := json.Unmarshal([]byte(mapping), &propertiesMap)
	if err != nil {
		return err
	}
	fmt.Println(propertiesMap)

	var NewIndexTypedMapping *types.TypeMapping = &types.TypeMapping{
		//Properties: map[string]types.Property{
		//	ContentFieldName: types.NewTextProperty(),
		//	"md5":            types.NewKeywordProperty(),
		//	"where":          types.NewKeywordProperty(),
		//},
		Properties: propertiesMap,
	}

	for _, indexName := range expectIndexList {
		//检查索引是否存在
		exists, err := s.esClient.Indices.Exists(indexName).IsSuccess(s.context)
		if err != nil {
			log.Fatal().Msgf("check index exist failed", err.Error())
		}

		//索引不存在则创建索引
		//索引不存在时查询会报错，但索引不存在的时候可以直接插入
		if !exists {
			log.Info().Msgf("index %s is not exist, to create", indexName)
			cr, err := s.esClient.Indices.Create(indexName).Request(&create.Request{
				Mappings: NewIndexTypedMapping,
			}).Do(s.context)
			if err != nil {
				log.Fatal().Msgf("create index failed", err.Error())
			}
			log.Info().Msgf("index create", cr.Index)
		}

		//// 设置索引的映射
		//var propertiesMap map[string]types.Property
		//err = json.Unmarshal([]byte(mapping), &propertiesMap)
		//if err != nil {
		//	return err
		//}
		//fmt.Println(propertiesMap)
		//req := s.esClient.Indices.PutMapping(indexName).Properties(propertiesMap)
		//res, err := req.Perform(s.context)
		//if err != nil {
		//	log.Info().Msgf("Error getting response: %s", err)
		//}
		//defer res.Body.Close()
		//
		//fmt.Println("Mapping created successfully.")
	}
	return nil
}

func (s *Service) EsDelete(docId string, index string) ([]byte, error) {
	res, err := s.esClient.Delete(index, docId).Perform(s.context)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		return nil, ErrQuery
	}
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (s *Service) EsInput(index string, document map[string]interface{}) ([]byte, error) {
	docId := uuid.NewString()
	fmt.Println(docId)
	fmt.Println(esutil.NewJSONReader(document))
	res, err := s.esClient.Index(index).Raw(esutil.NewJSONReader(document)).Id(docId).Perform(s.context)
	if err != nil {
		log.Fatal().Msgf("Error indexing document: %s", err)
	}
	defer res.Body.Close()

	fmt.Println("Document indexed successfully. Document ID:", docId)
	return []byte(docId), nil
}

func (s *Service) EsQueryByPath(indexName, path string) (*search.Response, error) {
	res, err := s.esClient.Search().
		Index(indexName).
		Request(&search.Request{
			Query: &types.Query{
				Term: map[string]types.TermQuery{
					"where": {Value: path},
				},
			},
		}).Do(s.context)
	if err != nil {
		return nil, fmt.Errorf("error when calling `SearchApi.Search``: %v", err)
	}
	for index, value := range res.Hits.Hits {
		fmt.Println(index, res.Fields, res.UnmarshalJSON(value.Source_))
	}
	fmt.Println(res.Hits.Total)
	return res, nil
}

func (s *Service) EsRawQuery(indexName, term string, size int) (*search.Response, error) {
	res, err := s.esClient.Search().
		Index(indexName).
		Request(&search.Request{
			Query: &types.Query{
				Bool: &types.BoolQuery{
					Should: []types.Query{
						{
							Match: map[string]types.MatchQuery{
								"content": {Query: term},
							},
						},
						{
							Match: map[string]types.MatchQuery{
								"format_name": {Query: term},
							},
						},
						{
							Match: map[string]types.MatchQuery{
								"name": {Query: term},
							},
						},
					},
				},
			},
			Size: &size,
			Highlight: &types.Highlight{
				Fields: map[string]types.HighlightField{
					"content": {},
				},
			},
		}).Do(s.context)
	if err != nil {
		return nil, fmt.Errorf("error when calling `SearchApi.Search``: %v", err)
	}
	for index, value := range res.Hits.Hits {
		fmt.Println(index, res.Fields, res.UnmarshalJSON(value.Source_))
	}
	fmt.Println(res.Hits.Total)
	return res, nil
}

func EsGetFileQueryResult(resp *search.Response) ([]FileQueryResult, error) {
	resultList := make([]FileQueryResult, 0)
	for _, hit := range resp.Hits.Hits {
		result := FileQueryResult{
			Index:       FileIndex,
			HightLights: make([]string, 0),
		}
		var data map[string]interface{}
		err := json.Unmarshal(hit.Source_, &data)
		if err != nil {
			fmt.Println("解析 Source 字段时出错:", err)
			continue
		}
		if where, ok := data["where"].(string); ok {
			result.Where = where
		}
		if md5, ok := data["md5"].(string); ok {
			result.Md5 = md5
		}
		if name, ok := data["name"].(string); ok {
			result.Name = name
		}
		result.DocId = hit.Id_
		if created, ok := data["created"].(float64); ok {
			result.Created = int64(created)
		}
		if updated, ok := data["updated"].(float64); ok {
			result.Updated = int64(updated)
		}
		if content, ok := data["content"].(string); ok {
			result.Content = content
		}
		result.Type = parser.GetTypeFromName(result.Name)
		if size, ok := data["size"].(float64); ok {
			result.Size = int64(size)
		}
		result.Modified = result.Created

		for _, highlightRes := range hit.Highlight {
			for _, h := range highlightRes {
				result.HightLights = append(result.HightLights, h)
			}
		}
		resultList = append(resultList, result)
	}
	return resultList, nil
}

func EsGetRssQueryResult(resp *search.Response) ([]RssQueryResult, error) {
	resultList := make([]RssQueryResult, 0)
	for _, hit := range resp.Hits.Hits {
		result := RssQueryResult{
			Index:       FileIndex,
			DocId:       hit.Id_,
			HightLights: make([]string, 0),
		}
		var data map[string]interface{}
		err := json.Unmarshal(hit.Source_, &data)
		if err != nil {
			fmt.Println("解析 Source 字段时出错:", err)
			continue
		}
		if name, ok := data["name"].(string); ok {
			result.Name = name
		}
		if created, ok := data["created"].(float64); ok {
			result.Created = int64(created)
		}
		if content, ok := data["content"].(string); ok {
			result.Content = content
		}
		if meta, ok := data["meta"].(string); ok {
			result.Meta = meta
		}

		for _, highlightRes := range hit.Highlight {
			for _, h := range highlightRes {
				result.HightLights = append(result.HightLights, h)
			}
		}
		resultList = append(resultList, result)
	}
	return resultList, nil
}

func (s *Service) EsQuery(index, term string, size int) ([]FileQueryResult, error) {
	res, err := s.EsRawQuery(index, term, size)
	if err != nil {
		return nil, err
	}
	return EsGetFileQueryResult(res)
}

//func (s *Service) EsGetContentByDocId(index, docId string) (string, error) {
//	url := s.zincUrl + "/es/" + index + "/_doc/" + docId
//	req, err := http.NewRequest("GET", url, strings.NewReader(""))
//	if err != nil {
//		return "", err
//	}
//	req.SetBasicAuth(s.username, s.password)
//	req.Header.Set("Content-Type", "application/json")
//	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36")
//
//	resp, err := http.DefaultClient.Do(req)
//	if err != nil {
//		return "", err
//	}
//	defer resp.Body.Close()
//	body, err := ioutil.ReadAll(resp.Body)
//	if err != nil {
//		return "", err
//	}
//	content := gjson.Get(string(body), "_source.content").String()
//	return content, nil
//}

func (s *Service) EsUpdateFileContentFromOldDoc(index, newContent, md5 string, oldDoc FileQueryResult) (string, error) {
	size := 0
	fileInfo, err := os.Stat(oldDoc.Where)
	if err == nil {
		size = int(fileInfo.Size())
	}
	newDoc := map[string]interface{}{
		"name":        oldDoc.Name,
		"where":       oldDoc.Where,
		"md5":         md5,
		"content":     newContent,
		"size":        size,
		"created":     oldDoc.Created,
		"updated":     time.Now().Unix(),
		"format_name": oldDoc.Name,
	}

	//ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
	//	UserName: s.username,
	//	Password: s.password,
	//})
	//resp, _, err := s.apiClient.Document.Update(ctx, index, oldDoc.DocId).Document(newDoc).Execute()
	res, err := s.esClient.Update(index, oldDoc.DocId).Doc(newDoc).Do(s.context)
	if err != nil {
		return "", err
	}
	fmt.Println(res)
	return "", nil
}

func TestInitES() {
	builtinlog.SetFlags(0)

	var (
	//r  map[string]interface{}
	//wg sync.WaitGroup
	)

	// Initialize a client with the default settings.
	//
	// An `ELASTICSEARCH_URL` environment variable will be used when exported.
	//
	//es, err := elasticsearch.NewDefaultClient()
	es, err := InitES("", "", "")
	if err != nil {
		builtinlog.Fatalf("Error creating the client: %s", err)
	}
	//ctx := context.TODO()
	ctx1 := context.WithValue(context.Background(), "Username", "admin")
	ctx := context.WithValue(ctx1, "Password", "User#123")

	// 1. Get cluster info
	//
	info := es.Info()
	fmt.Println(*info)
	//if err != nil {
	//	log.Fatalf("Error getting response: %s", err)
	//}
	//defer res.Body.Close()
	// Check response status
	//if res.IsError() {
	//	log.Fatalf("Error: %s", res.String())
	//}
	// Deserialize the response into a map.
	//if err := json.NewDecoder(res.Body()).Decode(&r); err != nil {
	//	log.Fatalf("Error parsing the response body: %s", err)
	//}
	// Print client and server version numbers.
	builtinlog.Printf("Client: %s", elasticsearch.Version)
	//log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])
	builtinlog.Println(strings.Repeat("~", 37))

	indexName := "test-index"
	var TestIndexTypedMapping *types.TypeMapping = &types.TypeMapping{
		Properties: map[string]types.Property{
			"id":      types.NewIntegerNumberProperty(),
			"title":   types.NewTextProperty(),
			"content": types.NewTextProperty(),
			"add_at":  types.NewKeywordProperty(),
		},
	}

	//检查索引是否存在
	exists, err := es.Indices.Exists(indexName).IsSuccess(ctx)
	if err != nil {
		builtinlog.Fatalln("check index exist failed", err.Error())
	}

	//索引不存在则创建索引
	//索引不存在时查询会报错，但索引不存在的时候可以直接插入
	if !exists {
		builtinlog.Printf("index %s is not exist, to create", indexName)
		cr, err := es.Indices.Create(indexName).Request(&create.Request{
			Mappings: TestIndexTypedMapping,
		}).Do(ctx)
		if err != nil {
			builtinlog.Fatal("create index failed", err.Error())
		}
		builtinlog.Println("index creat", cr.Index)
	}

	//type doc struct {
	//				Id      int    `json:"id"`
	//				Title   string `json:"title"`
	//				Content string `json:"content"`
	//				AddAt   string `json:"add_at"`
	//			}

	// 2. Index documents concurrently
	//
	//for i, title := range []string{"Test One Typed", "Test Two Typed"} {
	//	wg.Add(1)
	//
	//	go func(i int, title string) {
	//		defer wg.Done()
	//
	//		// Build the request body.
	//		//data, err := json.Marshal(struct {
	//		//	Title string `json:"title"`
	//		//}{Title: title})
	//		//if err != nil {
	//		//	log.Fatalf("Error marshaling document: %s", err)
	//		//}
	//
	//		// Set up the request object.
	//		//req := esapi.IndexRequest{
	//		//	Index:      "test",
	//		//	DocumentID: strconv.Itoa(i + 1),
	//		//	Body:       bytes.NewReader(data),
	//		//	Refresh:    "true",
	//		//}
	//		//
	//		//// Perform the request with the client.
	//		//res, err := req.Do(context.Background(), es)
	//
	// Set up the request object.
	document := struct {
		Id      int    `json:"id"`
		Title   string `json:"title"`
		Content string `json:"content"`
		AddAt   string `json:"add_at"`
	}{
		Id:      3,                          //i,
		Title:   "TestEsInput",              //title,
		Content: "TestEsInput bombing zinc", //title + " bombing zinc",
		AddAt:   "ByteTrade",
	}

	objType := reflect.TypeOf(document)
	objValue := reflect.ValueOf(document)

	if objType.Kind() == reflect.Ptr {
		objType = objType.Elem()
		objValue = objValue.Elem()
	}

	data := make(map[string]interface{})

	for i := 0; i < objValue.NumField(); i++ {
		field := objType.Field(i)
		value := objValue.Field(i).Interface()
		data[field.Name] = value
	}
	RpcServer.EsInput(indexName, data)
	//
	//		res, err := es.Index(indexName).
	//			Request(document).
	//			Do(context.Background())
	//
	//		if err != nil {
	//			log.Fatalf("Error getting response: %s", err)
	//		}
	//		fmt.Println(res)
	//		//defer res.Body.Close()
	//
	//		//if res.IsError() {
	//		//	log.Printf("[%s] Error indexing document ID=%d", res.Status(), i+1)
	//		//} else {
	//		//	// Deserialize the response into a map.
	//		//	var r map[string]interface{}
	//		//	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
	//		//		log.Printf("Error parsing the response body: %s", err)
	//		//	} else {
	//		//		// Print the response status and indexed document version.
	//		//		log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
	//		//	}
	//		//}
	//	}(i, title)
	//}
	//wg.Wait()
	//
	//log.Println(strings.Repeat("-", 37))

	// 3. Search for the indexed documents
	res, err := es.Search().
		Index("").
		Request(&search.Request{
			//Query: &types.Query{
			//	Match: map[string]types.MatchQuery{
			//		"content": {Query: "Test One "},
			//	},
			//},
		}).Do(ctx)
	for index, value := range res.Hits.Hits {
		fmt.Println(index, res.Fields, res.UnmarshalJSON(value.Source_))
	}
	fmt.Println(res.Hits.Total)

	type Hit struct {
		Index  string          `json:"_index"`
		ID     string          `json:"_id"`
		Score  float64         `json:"_score"`
		Source json.RawMessage `json:"_source"`
	}

	type SearchResponse struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []Hit `json:"hits"`
		} `json:"hits"`
	}

	for _, hit := range res.Hits.Hits {
		fmt.Println("索引名称:", hit.Index_)
		fmt.Println("文档 ID:", hit.Id_)
		fmt.Println("匹配分数:", hit.Score_)

		// 解析 Source 字段的 JSON 数据
		var data map[string]interface{}
		err := json.Unmarshal(hit.Source_, &data)
		if err != nil {
			fmt.Println("解析 Source 字段时出错:", err)
			continue
		}

		fmt.Println(data)
		fmt.Println("标题:", data["title"])
		fmt.Println("内容:", data["content"])
		fmt.Println("------")
	}
	////
	//// Build the request body.
	//var buf bytes.Buffer
	//query := map[string]interface{}{
	//	"query": map[string]interface{}{
	//		"match": map[string]interface{}{
	//			"title": "test",
	//		},
	//	},
	//}
	//if err := json.NewEncoder(&buf).Encode(query); err != nil {
	//	log.Fatalf("Error encoding query: %s", err)
	//}
	//
	//// Perform the search request.
	//res := es.Search(
	//	es.Search.WithContext(context.Background()),
	//	es.Search.WithIndex("test"),
	//	es.Search.WithBody(&buf),
	//	es.Search.WithTrackTotalHits(true),
	//	es.Search.WithPretty(),
	//)
	if err != nil {
		builtinlog.Fatalf("Error getting response: %s", err)
	}
	//defer res.Body.Close()
	//
	//if res.IsError() {
	//	var e map[string]interface{}
	//	if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
	//		log.Fatalf("Error parsing the response body: %s", err)
	//	} else {
	//		// Print the response status and error information.
	//		log.Fatalf("[%s] %s: %s",
	//			res.Status(),
	//			e["error"].(map[string]interface{})["type"],
	//			e["error"].(map[string]interface{})["reason"],
	//		)
	//	}
	//}

	//if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
	//	log.Fatalf("Error parsing the response body: %s", err)
	//}
	// Print the response status, number of results, and request duration.
	//log.Printf(
	//	"%d hits; took: %dms",
	//	//res.Status(),
	//	int(res.Hits().(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
	//	int(r["took"].(float64)),
	//)
	//// Print the ID and document source for each hit.
	//for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
	//	log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
	//}
	//fmt.Println(res)

	builtinlog.Println(strings.Repeat("=", 37))
}
