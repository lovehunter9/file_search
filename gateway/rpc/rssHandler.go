package rpc

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
)

type RssQueryResp struct {
	Count  int            `json:"count"`
	Offset int            `json:"offset"`
	Limit  int            `json:"limit"`
	Items  []RssQueryItem `json:"items"`
}

type FeedInfo struct {
	FeedId   int64  `json:"feed_id"`
	FeedName string `json:"feed_name"`
	FeedIcon string `json:"feed_icon"`
}

type Border struct {
	Name string `json:"name"`
	Id   int64  `json:"id"`
}

type RssMeta struct {
	Name      string     `json:"name"`
	EntryId   int64      `json:"entry_id"`
	Created   int64      `json:"created"`
	FeedInfos []FeedInfo `json:"feed_infos"`
	Borders   []Border   `json:"borders"`
}

type RssInputRequest struct {
	RssMeta
	Content string `json:"content"`
}

func (s *Service) HandleRssInput(c *gin.Context) {
	rep := Resp{
		ResultCode: ErrorCodeUnknow,
		ResultMsg:  "",
	}
	defer func() {
		if rep.ResultCode == Success {
			c.JSON(http.StatusOK, rep)
		}
	}()

	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		rep.ResultMsg = err.Error()
		c.JSON(http.StatusBadRequest, rep)
		return
	}
	rssInput := RssInputRequest{
		RssMeta: RssMeta{
			Name:      "",
			EntryId:   0,
			Created:   time.Now().Unix(),
			FeedInfos: make([]FeedInfo, 0),
			Borders:   make([]Border, 0),
		},
		Content: "",
	}
	log.Debug().Msgf("request body: %s", string(body))
	err = json.Unmarshal(body, &rssInput)
	if err != nil {
		log.Error().Msgf("json unmarshal request body error %s", err.Error())
		rep.ResultMsg = err.Error()
		c.JSON(http.StatusBadRequest, rep)
		return
	}

	metaInfo, _ := json.Marshal(&rssInput.RssMeta)
	doc := map[string]interface{}{
		"name":        rssInput.Name,
		"content":     rssInput.Content,
		"created":     time.Now().Unix(),
		"format_name": FormatFilename(rssInput.Name),
		"meta":        string(metaInfo),
	}

	log.Info().Msgf("add input rss index %s doc %v", RssIndex, doc)
	id, err := s.EsInput(RssIndex, doc)
	if err != nil {
		rep.ResultCode = ErrorCodeInput
		rep.ResultMsg = err.Error()
		log.Error().Msgf("input rss error %s", err.Error())
		c.JSON(http.StatusBadRequest, rep)
		return
	}
	rep.ResultCode = Success
	rep.ResultMsg = string(id)
}

func (s *Service) HandleRssDelete(c *gin.Context) {
	rep := Resp{
		ResultCode: ErrorCodeUnknow,
		ResultMsg:  "",
	}
	defer func() {
		if rep.ResultCode == Success {
			c.JSON(http.StatusOK, rep)
		}
	}()

	index := RssIndex
	docId := c.PostForm("docId")
	if docId == "" {
		rep.ResultCode = ErrorCodeDelete
		rep.ResultMsg = fmt.Sprintf("docId empty")
		c.JSON(http.StatusBadRequest, rep)
		return
	}
	log.Info().Msgf("zinc delete index %s docid%s", index, docId)
	_, err := s.EsDelete(docId, index)
	if err != nil {
		rep.ResultCode = ErrorCodeDelete
		rep.ResultMsg = err.Error()
		log.Error().Msgf("zinc delete error %s", err.Error())
		c.JSON(http.StatusBadRequest, rep)
		return
	}
	rep.ResultCode = Success
	rep.ResultMsg = docId
}

func (s *Service) HandleRssQuery(c *gin.Context) {
	rep := Resp{
		ResultCode: ErrorCodeUnknow,
		ResultMsg:  "",
	}

	defer func() {
		if rep.ResultCode == Success {
			c.JSON(http.StatusOK, rep)
		}
	}()

	index := RssIndex

	term := c.PostForm("query")
	if term == "" {
		rep.ResultMsg = "term empty"
		c.JSON(http.StatusBadRequest, rep)
		return
	}

	maxResults, err := strconv.Atoi(c.PostForm("limit"))
	if err != nil {
		maxResults = DefaultMaxResult
	}
	log.Info().Msgf("zinc query index %s term %s max %v", index, term, maxResults)
	//res, err := s.ZincRawQuery(index, term, int32(maxResults))
	res, err := s.EsRawQuery(index, term, maxResults)
	if err != nil {
		rep.ResultMsg = "zincsearch query error" + err.Error()
		log.Error().Msg(rep.ResultMsg)
		c.JSON(http.StatusBadRequest, rep)
		return
	}

	results, err := EsGetRssQueryResult(res)
	if err != nil {
		rep.ResultMsg = "zincsearch query error" + err.Error()
		log.Error().Msg(rep.ResultMsg)
		c.JSON(http.StatusBadRequest, rep)
		return
	}

	rep.ResultCode = Success
	items := slashRssQueryResult(results)
	response := RssQueryResp{
		Count:  len(items),
		Offset: 0,
		Limit:  maxResults,
		Items:  items,
	}
	repMsg, _ := json.Marshal(&response)
	rep.ResultMsg = string(repMsg)
}

type RssQueryItem struct {
	RssMeta
	DocId   string `json:"docId"`
	Snippet string `json:"snippet"`
}

func slashRssQueryResult(results []RssQueryResult) []RssQueryItem {
	itemsList := make([]RssQueryItem, len(results))
	for i, res := range results {
		itemsList[i] = shortRssQueryResult(res)
	}
	return itemsList
}

func shortRssQueryResult(res RssQueryResult) RssQueryItem {
	snippet := ""
	if len(res.HightLights) > 0 {
		snippet = res.HightLights[0]
	}
	meta := RssMeta{
		Name:      "",
		EntryId:   0,
		Created:   0,
		FeedInfos: make([]FeedInfo, 0),
		Borders:   make([]Border, 0),
	}
	json.Unmarshal([]byte(res.Meta), &meta)
	return RssQueryItem{
		RssMeta: RssMeta{
			Name:      res.Name,
			EntryId:   meta.EntryId,
			Created:   meta.Created,
			FeedInfos: meta.FeedInfos,
			Borders:   meta.Borders,
		},
		DocId:   res.DocId,
		Snippet: snippet,
	}
}
