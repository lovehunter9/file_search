package rpc

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	selfdriving "wzinc/ai/self-driving"
	"wzinc/common"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog/log"
	zinc "github.com/zinclabs/sdk-go-zincsearch"
)

const InternalError = "internal server error"

const (
	Success            = 0
	ErrorCodeUnknow    = -101
	ErrorCodeInput     = -102
	ErrorCodeDelete    = -103
	ErrorCodeUnmarshal = -104
	ErrorCodeTimeout   = -105
)

const (
	HealthCheckUrl = "/health"
	QuestionUrl    = "/api"
)

var SessionCookieName = "session_id"

var Host = "127.0.0.1"

const FileIndex = "Files"
const RssIndex = "Rss"
const DefaultMaxResult = 10

var once sync.Once

var RpcServer *Service

var maxPendingLength = 30

type Service struct {
	port             string
	zincUrl          string
	username         string
	password         string
	apiClient        *zinc.APIClient
	bsApiClient      map[string]*selfdriving.Client //modelname -> client
	questionCh       chan (common.PendingQuestion)
	maxPendingLength int
	CallbackGroup    *gin.RouterGroup
}

func InitRpcService(url, port, username, password string, bsModelConfig map[string]string) {
	once.Do(func() {
		configuration := zinc.NewConfiguration()
		configuration.Servers = zinc.ServerConfigurations{
			zinc.ServerConfiguration{
				URL: url,
			},
		}
		apiClient := zinc.NewAPIClient(configuration)

		RpcServer = &Service{
			port:             port,
			zincUrl:          url,
			username:         username,
			password:         password,
			apiClient:        apiClient,
			bsApiClient:      make(map[string]*selfdriving.Client),
			questionCh:       make(chan common.PendingQuestion),
			maxPendingLength: maxPendingLength,
		}

		//setup zinc index
		if err := RpcServer.setupIndex(); err != nil {
			panic(err)
		}

		//load ai model
		for modelName, url := range bsModelConfig {
			log.Info().Msgf("init model name:%s url:%s", modelName, url)
			RpcServer.bsApiClient[modelName] = selfdriving.NewClient(url, modelName, context.Background())
		}

		//load routes
		RpcServer.loadRoutes(context.Background())
	})
}

type LoggerMy struct {
}

func (*LoggerMy) Write(p []byte) (n int, err error) {
	msg := strings.TrimSpace(string(p))
	if strings.Index(msg, `"/healthcheck"`) > 0 {
		return
	}
	return
}

type Resp struct {
	ResultCode int    `json:"code"`
	ResultMsg  string `json:"data"`
}

var RpcEngine *gin.Engine

func (c *Service) Start(ctx context.Context) error {
	address := "0.0.0.0:" + c.port
	go RpcEngine.Run(address)
	log.Info().Msgf("start rpc on port:%s", c.port)
	return nil
}

func (c *Service) loadRoutes(ctx context.Context) error {
	//start ai question service
	postQuestionsContext, _ := context.WithCancel(ctx)
	go c.StartChatService(postQuestionsContext)

	//start gin
	gin.DefaultWriter = &LoggerMy{}
	RpcEngine = gin.Default()

	//cors middleware
	RpcEngine.SetTrustedProxies(nil)
	RpcEngine.GET("/healthcheck", func(c *gin.Context) {
		c.String(http.StatusOK, "ok")
	})

	RpcEngine.POST("/api/input", c.HandleInput)
	RpcEngine.POST("/api/delete", c.HandleDelete)
	RpcEngine.POST("/api/query", c.HandleQuery)

	RpcEngine.POST("/api/ai/question", c.HandleQuestion)

	c.CallbackGroup = RpcEngine.Group("/api/callback")
	log.Info().Msgf("init rpc server:%s")
	return nil
}

func (s *Service) HandleInput(c *gin.Context) {
	index := c.Query("index")
	if index != FileIndex && index != RssIndex {
		rep := Resp{
			ResultCode: ErrorCodeUnknow,
			ResultMsg:  fmt.Sprintf("only support index %s&%s", FileIndex, RssIndex),
		}
		c.JSON(http.StatusBadRequest, rep)
	}
	if index == FileIndex {
		s.HandleFileInput(c)
	}
	if index == RssIndex {
		s.HandleRssInput(c)
	}
}

func (s *Service) HandleDelete(c *gin.Context) {
	index := c.Query("index")
	if index != FileIndex && index != RssIndex {
		rep := Resp{
			ResultCode: ErrorCodeUnknow,
			ResultMsg:  fmt.Sprintf("only support index %s&%s", FileIndex, RssIndex),
		}
		c.JSON(http.StatusBadRequest, rep)
	}
	if index == FileIndex {
		s.HandleFileDelete(c)
	}
	if index == RssIndex {
		s.HandleRssDelete(c)
	}
}

func (s *Service) HandleQuery(c *gin.Context) {
	index := c.Query("index")
	if index != FileIndex && index != RssIndex {
		rep := Resp{
			ResultCode: ErrorCodeUnknow,
			ResultMsg:  fmt.Sprintf("only support index %s&%s", FileIndex, RssIndex),
		}
		c.JSON(http.StatusBadRequest, rep)
	}
	if index == FileIndex {
		s.HandleFileQuery(c)
	}
	if index == RssIndex {
		s.HandleRssQuery(c)
	}
}
