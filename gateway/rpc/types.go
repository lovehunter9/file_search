package rpc

import "wzinc/common"

type pendingQuestion struct {
	data       common.Question
	resp       chan (RelayResponse)
	cancel     chan (struct{})
}

type RelayResponse struct {
	Url            string `json:"url"` //bs url or openai key
	Text           string `json:"text"`
	MessageId      string `json:"messageId"`
	ConversationId string `json:"conversationId"`
	Model          string `json:"model"`
}

var emptyStatus UserStatus

type UserStatus struct {
	MessageId      string `json:"messageId"`
	ConversationId string `json:"conversationId"`
	Url            string `json:"url"` //bs url
	LastTime       int64  `json:"lastTime"`
	Model          string `json:"model"`
}

type ProxyResponse struct {
	Text           string `json:"text"`
	MessageId      string `json:"messageId"`
	ConversationId string `json:"conversationId"`
	Model          string `json:"model"`
}
