package rpc

//const ContentFieldName = "content"

//type QueryReq struct {
//	SearchType string `json:"search_type"`
//	Query      Query  `json:"query"`
//	From       int    `json:"from"`
//	MaxResult  int    `json:"max_results"`
//}
//
//type Query struct {
//	Term string `json:"term"`
//}

//var ErrQuery = errors.New("query err")

//func (s *Service) ZincDelete(docId string, index string) ([]byte, error) {
//	url := s.zincUrl + "/api/" + index + "/_doc/" + docId
//	req, err := http.NewRequest("DELETE", url, strings.NewReader(""))
//	if err != nil {
//		return nil, err
//	}
//	req.SetBasicAuth(s.username, s.password)
//	req.Header.Set("Content-Type", "application/json")
//	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36")
//
//	resp, err := http.DefaultClient.Do(req)
//	if err != nil {
//		return nil, err
//	}
//	defer resp.Body.Close()
//	if resp.StatusCode != 200 {
//		return nil, ErrQuery
//	}
//	body, err := ioutil.ReadAll(resp.Body)
//	if err != nil {
//		return nil, err
//	}
//	return body, nil
//}

//func (s *Service) ZincInput(index string, document map[string]interface{}) ([]byte, error) {
//	id := uuid.NewString()
//	ctx := context.WithValue(context.TODO(), zinc.ContextBasicAuth, zinc.BasicAuth{
//		UserName: s.username,
//		Password: s.password,
//	})
//	resp, _, err := s.apiClient.Document.IndexWithID(ctx, index, id).Document(document).Execute()
//	if err != nil {
//		return nil, err
//	}
//	return []byte(resp.GetId()), nil
//}

//type Document struct {
//	FilePath string `json:"filepath"`
//	FileName string `json:"filename"`
//	Content  string `json:"content"`
//}

//type FileQueryResult struct {
//	Index       string   `json:"index"`
//	Where       string   `json:"where"`
//	Md5         string   `json:"md5"`
//	Name        string   `json:"name"`
//	DocId       string   `json:"docId"`
//	Created     int64    `json:"created"`
//	Updated     int64    `json:"updated"`
//	Content     string   `json:"content"`
//	Type        string   `json:"type"`
//	Size        int64    `json:"size"`
//	Modified    int64    `json:"modified"`
//	HightLights []string `json:"highlight"`
//}

// doc := map[string]interface{}{
// 	"name":        rssInput.Name,
// 	"content":     rssInput.Content,
// 	"created":     time.Now().Unix(),
// 	"format_name": FormatFilename(rssInput.Name),
// 	"meta":        metaInfo,
// }

//type RssQueryResult struct {
//	Index       string   `json:"index"`
//	Name        string   `json:"name"`
//	DocId       string   `json:"docId"`
//	Created     int64    `json:"created"`
//	Content     string   `json:"content"`
//	Meta        string   `json:"meta"`
//	HightLights []string `json:"highlight"`
//}

//func (s *Service) ZincQueryByPath(indexName, path string) (*zinc.MetaSearchResponse, error) {
//	query := *zinc.NewMetaZincQuery()
//	termPathQuery := *zinc.NewMetaTermQuery()
//	termPathQuery.SetValue(path)
//	queryQuery := *zinc.NewMetaQuery()
//	queryQuery.SetTerm(map[string]zinc.MetaTermQuery{
//		"where": termPathQuery,
//	})
//	query.SetQuery(queryQuery)
//	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
//		UserName: s.username,
//		Password: s.password,
//	})
//	resp, _, err := s.apiClient.Search.Search(ctx, indexName).Query(query).Execute()
//	if err != nil {
//		return nil, fmt.Errorf("error when calling `SearchApi.Search``: %v", err)
//	}
//	return resp, nil
//}

//func (s *Service) ZincRawQuery(indexName, term string, size int32) (*zinc.MetaSearchResponse, error) {
//	query := *zinc.NewMetaZincQuery()
//	query.SetSize(size)
//	highlight := zinc.NewMetaHighlight()
//	highlightContent := zinc.NewMetaHighlight()
//	highlight.SetFields(map[string]zinc.MetaHighlight{"content": *highlightContent})
//	query.SetHighlight(*highlight)
//
//	matchQuery := *zinc.NewMetaMatchQuery()
//	matchQuery.SetQuery(term)
//	subQueryContent := *zinc.NewMetaQuery()
//	subQueryContent.SetMatch(map[string]zinc.MetaMatchQuery{
//		"content": matchQuery,
//	})
//	subQueryFormatName := *zinc.NewMetaQuery()
//	subQueryFormatName.SetMatch(map[string]zinc.MetaMatchQuery{
//		"format_name": matchQuery,
//	})
//	subQueryFileName := *zinc.NewMetaQuery()
//	subQueryFileName.SetMatch(map[string]zinc.MetaMatchQuery{
//		"name": matchQuery,
//	})
//	boolQuery := *zinc.NewMetaBoolQuery()
//	boolQuery.SetShould([]zinc.MetaQuery{subQueryContent, subQueryFormatName, subQueryFileName})
//	queryQuery := *zinc.NewMetaQuery()
//	queryQuery.SetBool(boolQuery)
//	query.SetQuery(queryQuery)
//
//	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
//		UserName: s.username,
//		Password: s.password,
//	})
//	resp, _, err := s.apiClient.Search.Search(ctx, indexName).Query(query).Execute()
//	if err != nil {
//		return nil, fmt.Errorf("error when calling `SearchApi.Search``: %v", err)
//	}
//	return resp, nil
//}

//func GetFileQueryResult(resp *zinc.MetaSearchResponse) ([]FileQueryResult, error) {
//	resultList := make([]FileQueryResult, 0)
//	for _, hit := range resp.Hits.Hits {
//		result := FileQueryResult{
//			Index:       FileIndex,
//			HightLights: make([]string, 0),
//		}
//		if where, ok := hit.Source["where"].(string); ok {
//			result.Where = where
//		}
//		if md5, ok := hit.Source["md5"].(string); ok {
//			result.Md5 = md5
//		}
//		if name, ok := hit.Source["name"].(string); ok {
//			result.Name = name
//		}
//		result.DocId = *hit.Id
//		if created, ok := hit.Source["created"].(float64); ok {
//			result.Created = int64(created)
//		}
//		if updated, ok := hit.Source["updated"].(float64); ok {
//			result.Updated = int64(updated)
//		}
//		if content, ok := hit.Source["content"].(string); ok {
//			result.Content = content
//		}
//		result.Type = parser.GetTypeFromName(result.Name)
//		if size, ok := hit.Source["size"].(float64); ok {
//			result.Size = int64(size)
//		}
//		result.Modified = result.Created
//
//		for _, highlightRes := range hit.Highlight {
//			for _, h := range highlightRes.([]interface{}) {
//				result.HightLights = append(result.HightLights, h.(string))
//			}
//		}
//		resultList = append(resultList, result)
//	}
//	return resultList, nil
//}

//func GetRssQueryResult(resp *zinc.MetaSearchResponse) ([]RssQueryResult, error) {
//	resultList := make([]RssQueryResult, 0)
//	for _, hit := range resp.Hits.Hits {
//		result := RssQueryResult{
//			Index:       FileIndex,
//			DocId:       *hit.Id,
//			HightLights: make([]string, 0),
//		}
//		if name, ok := hit.Source["name"].(string); ok {
//			result.Name = name
//		}
//		if created, ok := hit.Source["created"].(float64); ok {
//			result.Created = int64(created)
//		}
//		if content, ok := hit.Source["content"].(string); ok {
//			result.Content = content
//		}
//		if meta, ok := hit.Source["meta"].(string); ok {
//			result.Meta = meta
//		}
//
//		for _, highlightRes := range hit.Highlight {
//			for _, h := range highlightRes.([]interface{}) {
//				result.HightLights = append(result.HightLights, h.(string))
//			}
//		}
//		resultList = append(resultList, result)
//	}
//	return resultList, nil
//}

//func (s *Service) zincQuery(index, term string, size int32) ([]FileQueryResult, error) {
//	res, err := s.ZincRawQuery(index, term, size)
//	if err != nil {
//		return nil, err
//	}
//	return GetFileQueryResult(res)
//}

//func (s *Service) listIndex() ([]string, error) {
//	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
//		UserName: s.username,
//		Password: s.password,
//	})
//	resp, r, err := s.apiClient.Index.IndexNameList(ctx).Execute()
//	if err != nil {
//		return nil, err
//	}
//	if r.StatusCode != 200 {
//		return nil, fmt.Errorf("full HTTP response: %v", r)
//	}
//	return resp, nil
//}
//
//func (s *Service) createIndex(indexName string) error {
//	index := *zinc.NewMetaIndexSimple()
//	index.SetName(indexName)
//
//	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
//		UserName: s.username,
//		Password: s.password,
//	})
//
//	_, r, err := s.apiClient.Index.Create(ctx).Data(index).Execute()
//	if err != nil {
//		return err
//	}
//	// response from `Create`: MetaHTTPResponseIndex
//	if r.StatusCode != 200 {
//		e, _ := err.(*zinc.GenericOpenAPIError)
//		me, _ := e.Model().(zinc.MetaHTTPResponseError)
//		return fmt.Errorf("`Index.Create` error: %v", me.GetError())
//	}
//	log.Info().Msgf("setting index config mapping %s", indexName)
//	return s.setIndexMapping(indexName)
//}
//
//func (s *Service) setupIndex() error {
//	existIndexNameList, err := s.listIndex()
//	if err != nil {
//		return err
//	}
//	nameMap := make(map[string]bool)
//	for _, existName := range existIndexNameList {
//		nameMap[existName] = true
//		log.Info().Msgf("index %s exist", existName)
//	}
//
//	expectIndexList := []string{RssIndex, FileIndex}
//	for _, indexName := range expectIndexList {
//		if _, ok := nameMap[indexName]; !ok {
//			log.Info().Msgf("creating index %s", indexName)
//			err = s.createIndex(indexName)
//			if err != nil {
//				return err
//			}
//		}
//	}
//	return nil
//}
//
//// add highlightable filed "content" in index map setting
//func (s *Service) setIndexMapping(indexName string) error {
//	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
//		UserName: s.username,
//		Password: s.password,
//	})
//
//	mapping := *zinc.NewMetaMappings() // MetaMappings | Mapping
//
//	content := zinc.NewMetaProperty()
//	content.SetType("text")
//	content.SetIndex(true)
//	content.SetHighlightable(true)
//	content.SetAggregatable(false)
//	content.SetSortable(false)
//	content.SetStore(false)
//
//	where := zinc.NewMetaProperty()
//	where.SetType("text")
//	where.SetIndex(true)
//	where.SetHighlightable(false)
//	where.SetAggregatable(false)
//	where.SetAnalyzer("keyword")
//
//	md5 := zinc.NewMetaProperty()
//	md5.SetType("text")
//	md5.SetHighlightable(false)
//	md5.SetAggregatable(false)
//	md5.SetAnalyzer("keyword")
//
//	mapping.SetProperties(map[string]zinc.MetaProperty{
//		ContentFieldName: *content,
//		"where":          *where,
//		"md5":            *md5,
//	})
//
//	_, r, err := s.apiClient.Index.SetMapping(ctx, indexName).Mapping(mapping).Execute()
//	if err != nil {
//		return err
//	}
//	if r.StatusCode != 200 {
//		e, _ := err.(*zinc.GenericOpenAPIError)
//		me, _ := e.Model().(zinc.MetaHTTPResponseError)
//		return fmt.Errorf("`Index.SetMapping` error: %v", me.GetError())
//	}
//	return nil
//}

//func (s *Service) GetContentByDocId(index, docId string) (string, error) {
//	url := s.zincUrl + "/api/" + index + "/_doc/" + docId
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

//func (s *Service) UpdateFileContentFromOldDoc(index, newContent, md5 string, oldDoc FileQueryResult) (string, error) {
//	size := 0
//	fileInfo, err := os.Stat(oldDoc.Where)
//	if err == nil {
//		size = int(fileInfo.Size())
//	}
//	newDoc := map[string]interface{}{
//		"name":        oldDoc.Name,
//		"where":       oldDoc.Where,
//		"md5":         md5,
//		"content":     newContent,
//		"size":        size,
//		"created":     oldDoc.Created,
//		"updated":     time.Now().Unix(),
//		"format_name": oldDoc.Name,
//	}
//
//	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
//		UserName: s.username,
//		Password: s.password,
//	})
//	resp, _, err := s.apiClient.Document.Update(ctx, index, oldDoc.DocId).Document(newDoc).Execute()
//	if err != nil {
//		return "", err
//	}
//	return resp.GetId(), nil
//}

//func (s *Service) UpdateFileContentByPath(index, path, md5, newContent string) (string, error) {
//	res, err := s.ZincQueryByPath(index, path)
//	if err != nil {
//		return "", err
//	}
//	docData, err := GetFileQueryResult(res)
//	if err != nil {
//		return "", err
//	}
//	if len(docData) == 0 {
//		return "", errors.New("no doc")
//	}
//	oldDoc := docData[0]
//	size := 0
//	fileInfo, err := os.Stat(path)
//	if err == nil {
//		size = int(fileInfo.Size())
//	}
//	newDoc := map[string]interface{}{
//		"name":        oldDoc.Name,
//		"where":       oldDoc.Where,
//		"md5":         md5,
//		"content":     newContent,
//		"size":        size,
//		"created":     oldDoc.Created,
//		"updated":     time.Now().Unix(),
//		"format_name": oldDoc.Name,
//	}
//
//	ctx := context.WithValue(context.Background(), zinc.ContextBasicAuth, zinc.BasicAuth{
//		UserName: s.username,
//		Password: s.password,
//	})
//	resp, _, err := s.apiClient.Document.Update(ctx, index, oldDoc.DocId).Document(newDoc).Execute()
//	if err != nil {
//		return "", err
//	}
//	return resp.GetId(), nil
//}
