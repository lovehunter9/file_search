package rpc

//
//import (
//	"context"
//	"encoding/json"
//	"net/http"
//	"os"
//	"time"
//	selfdriving "wzinc/ai/self-driving"
//	"wzinc/common"
//	"wzinc/db"
//	"wzinc/parser"
//
//	"github.com/gin-gonic/gin"
//	"github.com/google/uuid"
//	"github.com/rs/zerolog/log"
//)
//
//const SensitiveResponse = "Sorry, as an artificial intelligence, I am unable to provide you with a standardized and satisfactory answer to your question. We will continuously optimize the system to provide better service. Thank you for your question."
//const WaitForAIAnswer = time.Minute * 60
//const ChatModelName = "chat_model"
//const FileModelName = "file_model"
//const PostCallbackTimeout = 60
//
//type FirstResponse struct {
//	ConversationId string `json:"conversationId"`
//	MessageId      string `json:"messageId"`
//	CallbackUri    string `json:"callback"`
//}
//
//func (s *Service) StartChatService(ctx context.Context) {
//	//max concurrent questions
//	handling := make(chan struct{}, s.maxPendingLength)
//	for {
//		select {
//		case <-ctx.Done():
//			return
//		case qu := <-s.questionCh:
//			handling <- struct{}{}
//			log.Debug().Msgf("try send question to relay %s", qu.Data)
//			go func() {
//				s.checkOneQuestion(qu)
//				log.Debug().Msgf("question to relay done %s", qu.Data)
//				<-handling
//			}()
//		}
//	}
//}
//
//func (s *Service) checkOneQuestion(qu common.PendingQuestion) {
//	defer close(qu.Finish)
//	if client, ok := s.bsApiClient[qu.Data.Model]; ok {
//		ctx, cancel := context.WithCancel(qu.Ctx)
//		defer cancel()
//		err := client.GetAnswer(ctx, qu)
//		if err != nil {
//			log.Error().Msgf("handle bs question error %s", err.Error())
//		}
//		return
//	}
//	log.Error().Msgf("model not exist name%s", qu.Data.Model)
//}
//
//func (s *Service) HandleQuestion(c *gin.Context) {
//	statusCode := http.StatusBadRequest
//	rep := Resp{
//		ResultCode: ErrorCodeUnknow,
//		ResultMsg:  "",
//	}
//	defer func() {
//		if rep.ResultCode == Success {
//			statusCode = http.StatusOK
//		}
//		c.JSON(statusCode, rep)
//	}()
//
//	//get session state
//	msg := c.PostForm("message")
//	if msg == "" {
//		rep.ResultMsg = "no question message"
//		log.Error().Msg(rep.ResultMsg)
//		return
//	}
//
//	conv_id := c.PostForm("conversationId")
//	filePath := c.PostForm("path")
//	callbackUri := c.PostForm("callback")
//	typeStr := c.PostForm("type")
//	typeStr = selfdriving.BasicOption
//	modelName := ChatModelName
//	if filePath != "" {
//		if filePath == selfdriving.FullDocOption {
//			typeStr = selfdriving.FullDocOption
//		} else {
//			typeStr = selfdriving.SingleDocOption
//			modelName = FileModelName
//			fileInfo, err := os.Stat(filePath)
//			if err != nil {
//				rep.ResultMsg = err.Error()
//				log.Error().Msg(rep.ResultMsg)
//				return
//			}
//			if !parser.IsParseAble(fileInfo.Name()) {
//				rep.ResultMsg = "file not parsable"
//				log.Error().Msg(rep.ResultMsg)
//				return
//			}
//		}
//	}
//
//	if conv_id == "" {
//		conv_id = uuid.NewString()
//	}
//
//	q := common.Question{
//		Message:        msg,
//		MessageId:      uuid.NewString(),
//		ConversationId: conv_id,
//		Model:          modelName,
//		FilePath:       filePath,
//		Type:           typeStr,
//	}
//	log.Debug().Msgf("new AI question: %v", q)
//	ctx, _ := context.WithTimeout(context.Background(), WaitForAIAnswer)
//	go s.questionCallback(ctx, q, callbackUri)
//	rep.ResultCode = Success
//	res := FirstResponse{
//		ConversationId: q.ConversationId,
//		MessageId:      q.MessageId,
//		CallbackUri:    callbackUri,
//	}
//	b, _ := json.Marshal(&res)
//	rep.ResultMsg = string(b)
//}
//
//func (s *Service) questionCallback(ctx context.Context, q common.Question, callbackUri string) {
//	pendingCtx, cancel := context.WithCancel(ctx)
//	defer cancel()
//	pendingQ := common.PendingQuestion{
//		Data:   q,
//		Chunk:  make(chan common.RelayResponse),
//		Finish: make(chan common.AnswerStreamFinish, 1),
//		Ctx:    pendingCtx,
//	}
//	totalAnswer := ""
//	select {
//	case <-ctx.Done():
//		log.Error().Msgf("pending question timeout %v", pendingQ.Data)
//		//todo handle timeout
//		resp := Resp{
//			ResultCode: ErrorCodeTimeout,
//			ResultMsg:  "pending question timeout",
//		}
//		b, _ := json.Marshal(&resp)
//		log.Debug().Msgf("post callback %s body %s", callbackUri, string(b))
//		_, err := common.HttpPost(callbackUri, string(b), PostCallbackTimeout)
//		if err != nil {
//			log.Error().Msgf("post callback %s body %s err %s", callbackUri, string(b), err.Error())
//		}
//		return
//
//	case s.questionCh <- pendingQ:
//		for {
//			select {
//			case <-ctx.Done():
//				log.Error().Msgf("wait AI output timeout %v", pendingQ.Data)
//				resp := Resp{
//					ResultCode: ErrorCodeTimeout,
//					ResultMsg:  "wait AI output timeout",
//				}
//				b, _ := json.Marshal(&resp)
//				log.Debug().Msgf("post callback %s body %s", callbackUri, string(b))
//				_, err := common.HttpPost(callbackUri, string(b), PostCallbackTimeout)
//				if err != nil {
//					log.Error().Msgf("post callback %s body %s err %s", callbackUri, string(b), err.Error())
//				}
//				return
//
//			case finish := <-pendingQ.Finish:
//				if finish.ErrorMsg != "" {
//					//error happened incorrectly finish
//					log.Error().Msgf("finish with error %v %s", pendingQ.Data, finish.ErrorMsg)
//					resp := Resp{
//						ResultCode: ErrorCodeUnknow,
//						ResultMsg:  finish.ErrorMsg,
//					}
//					b, _ := json.Marshal(&resp)
//					log.Debug().Msgf("post callback %s body %s", callbackUri, string(b))
//					_, err := common.HttpPost(callbackUri, string(b), PostCallbackTimeout)
//					if err != nil {
//						log.Error().Msgf("post callback %s body %s err %s", callbackUri, string(b), err.Error())
//					}
//					return
//				}
//				if finish.MessageId == "" {
//					//error happened incorrectly finish
//					log.Error().Msgf("finish messageid empty %v", pendingQ.Data)
//					resp := Resp{
//						ResultCode: ErrorCodeUnknow,
//						ResultMsg:  "finish messageId empty",
//					}
//					b, _ := json.Marshal(&resp)
//					log.Debug().Msgf("post callback %s body %s", callbackUri, string(b))
//					_, err := common.HttpPost(callbackUri, string(b), PostCallbackTimeout)
//					if err != nil {
//						log.Error().Msgf("post callback %s body %s err %s", callbackUri, string(b), err.Error())
//					}
//					return
//				}
//				//graceful finish
//				log.Debug().Msgf("AI finish output question: %s answer:%s", pendingQ.Data.Message, totalAnswer)
//				callBackResp := common.CallbackResponse{
//					Text:           totalAnswer,
//					MessageId:      finish.MessageId,
//					ConversationId: finish.ConversationId,
//					Model:          finish.Model,
//					Done:           true,
//				}
//				msg, _ := json.Marshal(&callBackResp)
//				resp := Resp{
//					ResultCode: Success,
//					ResultMsg:  string(msg),
//				}
//				b, _ := json.Marshal(&resp)
//				log.Debug().Msgf("post callback %s body %s", callbackUri, string(b))
//				_, err := common.HttpPost(callbackUri, string(b), PostCallbackTimeout)
//				if err != nil {
//					log.Error().Msgf("post callback %s body %s err %s", callbackUri, string(b), err.Error())
//				}
//				go func() {
//					err := db.InsertSingleConversation(db.Message{
//						ConversationId: finish.ConversationId,
//						MessageId:      finish.MessageId,
//						Prompt:         q.Message,
//						Text:           totalAnswer,
//						StartTime:      time.Now().Unix(),
//						Model:          finish.Model,
//					})
//					if err != nil {
//						log.Error().Msgf("insert into db error %s", err.Error())
//					}
//				}()
//				return
//
//			//new stream text
//			case answer := <-pendingQ.Chunk:
//				if totalAnswer == answer.Text {
//					continue
//				}
//				totalAnswer = answer.Text
//				log.Debug().Msgf("output chunk: %s", answer.Text)
//				callBackResp := common.CallbackResponse{
//					Text:           totalAnswer,
//					MessageId:      answer.MessageId,
//					ConversationId: answer.ConversationId,
//					Model:          answer.Model,
//					Done:           false,
//				}
//				msg, _ := json.Marshal(&callBackResp)
//				resp := Resp{
//					ResultCode: Success,
//					ResultMsg:  string(msg),
//				}
//				b, _ := json.Marshal(&resp)
//				log.Debug().Msgf("post callback %s body %s", callbackUri, string(b))
//				_, err := common.HttpPost(callbackUri, string(b), PostCallbackTimeout)
//				if err != nil {
//					log.Error().Msgf("post callback %s body %s err %s", callbackUri, string(b), err.Error())
//				}
//				continue
//			}
//		}
//	}
//}
