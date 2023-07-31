package inotify

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"os"
	"path"
	"strings"
	"time"
)

var difyHeaders map[string]string = nil
var datasetId string = ""

func TestHttps() {
	var body struct {
		Query string `json:"query"`
	}

	body.Query = "seafile"

	headers := make(map[string]string)

	statusCode, respBody, _, _ := JSONWithResp("http://127.0.0.1:6317/api/query?index=Files",
		"POST",
		headers,
		body,
		time.Duration(time.Second*10))

	fmt.Println(statusCode, string(respBody))
}

func JSONWithResp(url string, method string, headers map[string]string, data interface{}, timeout time.Duration) (statusCode int, respBody []byte, respHeader map[string][]string, err error) {

	var jsonBytes []byte
	if bytes, ok := data.([]byte); ok {
		jsonBytes = bytes
	} else {
		if jsonBytes, err = json.Marshal(data); err != nil {
			return
		}
	}

	var req *http.Request
	if req, err = http.NewRequest(method, url, bytes.NewBuffer(jsonBytes)); err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")

	if headers != nil {
		for key, value := range headers {
			req.Header.Set(key, value)
		}
	}

	tr := &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}} //不验证ca证书，否则卡在这里
	client := &http.Client{Timeout: timeout, Transport: tr}
	resp, err := client.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()

	statusCode = resp.StatusCode
	respBody, _ = ioutil.ReadAll(resp.Body)
	respHeader = resp.Header

	return
}

func GetDifyHeaders() {
	var body struct {
		Email      string `json:"email"`
		Password   string `json:"password"`
		RememberMe bool   `json:"remember_me"`
	}

	body.Email = os.Getenv("DIFY_USER_EMAIL")
	body.Password = os.Getenv("DFIY_USER_PASSWORD")
	body.RememberMe = true

	headers := make(map[string]string)

	_, _, respHeader, _ := JSONWithResp("http://localhost/console/api/login",
		"POST",
		headers,
		body,
		time.Duration(time.Second*10))

	//fmt.Println(statusCode, respHeader, string(respBody))

	remember_token := strings.Split(strings.Split(respHeader["Set-Cookie"][0], ";")[0], "=")[1]
	session := strings.Split(strings.Split(respHeader["Set-Cookie"][1], ";")[0], "=")[1]
	difyHeaders = map[string]string{
		"Cookie": "remeber_token=" + remember_token + "; session=" + session,
	}
	return
}

func IsDocumentExist() {
	_, respBody, _, _ := JSONWithResp("http://localhost/console/api/datasets",
		"GET",
		difyHeaders,
		nil,
		time.Duration(time.Second*10))
	//fmt.Println(string(respBody))

	var myRespBody map[string]interface{}
	err := json.Unmarshal([]byte(respBody), &myRespBody)
	if err != nil {
		fmt.Println(err)
	}
	//fmt.Println(myRespBody["data"])
	datasets := myRespBody["data"].([]interface{})
	//fmt.Println(datasets)
	for _, value := range datasets {
		valueTmp := value.(map[string]interface{})
		if valueTmp["name"].(string) == "Document" {
			datasetId = valueTmp["id"].(string)
		}
	}
	return
}

func CreateDocument() {
	var body struct {
		Name string `json:"name"`
	}

	body.Name = "Document"

	statusCode, respBody, respHeader, _ := JSONWithResp("http://localhost/console/api/datasets",
		"POST",
		difyHeaders,
		body,
		time.Duration(time.Second*10))

	fmt.Println(statusCode, respHeader, string(respBody))

	var myRespBody map[string]interface{}
	err := json.Unmarshal([]byte(respBody), &myRespBody)
	if err != nil {
		fmt.Println(err)
	}
	datasetId = myRespBody["id"].(string)
}

func InitDify() {
	if difyHeaders == nil {
		GetDifyHeaders()
	}
	IsDocumentExist()
	if datasetId == "" {
		CreateDocument()
	}
}

func postFile(filename string, target_url string, headers map[string]string) (*http.Response, error) {

	body_buf := bytes.NewBufferString("")
	body_writer := multipart.NewWriter(body_buf)

	// use the body_writer to write the Part headers to the buffer
	//paths := strings.Split(filename, "/")
	//_, err := body_writer.CreateFormFile("file", paths[len(paths)-1])
	_, err := body_writer.CreateFormFile("file", path.Base(filename))
	if err != nil {
		fmt.Println("error writing to buffer")
		return nil, err
	}

	// the file data will be the second part of the body
	fh, err := os.Open(filename)
	if err != nil {
		fmt.Println("error opening file")
		return nil, err
	}

	// need to know the boundary to properly close the part myself.
	boundary := body_writer.Boundary()

	//close_string := fmt.Sprintf("\r\n--%s--\r\n", boundary)
	close_buf := bytes.NewBufferString(fmt.Sprintf("\r\n--%s--\r\n", boundary))

	// use multi-reader to defer the reading of the file data until
	// writing to the socket buffer.
	request_reader := io.MultiReader(body_buf, fh, close_buf)
	fi, err := fh.Stat()
	if err != nil {
		fmt.Printf("Error Stating file: %s", filename)
		return nil, err
	}
	req, err := http.NewRequest("POST", target_url, request_reader)
	if err != nil {
		return nil, err
	}

	// Set headers for multipart, and Content Length
	req.Header.Add("Content-Type", "multipart/form-data; boundary="+boundary)
	req.ContentLength = fi.Size() + int64(body_buf.Len()) + int64(close_buf.Len())
	if headers != nil {
		for key, value := range headers {
			req.Header.Set(key, value)
		}
	}
	fmt.Println(req.Header)
	fmt.Println(req.Body)
	return http.DefaultClient.Do(req)
}

func UploadFile(fileName string) (fileId string) {
	resp, _ := postFile( // "/Users/wangrongxiang/Documents/seafile-ce-env-latest镜像使用方法.pdf",
		fileName,
		"http://localhost/console/api/files/upload",
		difyHeaders)

	statusCode := resp.StatusCode
	respBody, _ := ioutil.ReadAll(resp.Body)
	respHeader := resp.Header
	fmt.Println(statusCode, string(respBody), respHeader)

	if statusCode != 201 {
		return ""
	}
	var myRespBody map[string]interface{}
	err := json.Unmarshal([]byte(respBody), &myRespBody)
	if err != nil {
		fmt.Println(err)
	}
	fileId = myRespBody["id"].(string)
	return
}

func IndexerEstimate(fileId string) {
	// body example:
	//{
	//	"info_list": {
	//		"data_source_type": "upload_file",
	//		"file_info_list": {
	//			"file_ids": ["89e6269a-86f7-41aa-8546-7c574bf14bdd"]
	//		}
	//	},
	//	"indexing_technique": "economy",
	//	"process_rule": {
	//		"rules": {
	//			"pre_processing_rules": [
	//				{"id":"remove_extra_spaces","enabled":true},
	//				{"id":"remove_urls_emails","enabled":true}
	//			],
	//			"segmentation": {
	//				"separator":"\n",
	//				"max_tokens":500
	//			}
	//		},
	//		"mode": "custom"
	//	}
	//}

	var body struct {
		IndexingTechnique string `json:"indexing_technique"`
		InfoList          struct {
			DataSourceType string `json:"data_source_type"`
			FileInfoList   struct {
				FileIds []string `json:"file_ids"`
			} `json:"file_info_list"`
		} `json:"info_list"`
		ProcessRule struct {
			Mode  string   `json:"mode"`
			Rules struct{} `json:"rules"`
		} `json:"process_rule"`
	}

	body.IndexingTechnique = "economy"
	body.InfoList.DataSourceType = "upload_file"
	body.InfoList.FileInfoList.FileIds = []string{fileId}
	body.ProcessRule.Mode = "automatic"

	statusCode, respBody, respHeader, _ := JSONWithResp("http://localhost/console/api/datasets/indexing-estimate",
		"POST",
		difyHeaders,
		body,
		time.Duration(time.Second*10))

	fmt.Println(statusCode, respHeader, string(respBody))
}

func DatasetsDocument(fileId string) {
	//body example:
	//{
	//	"data_source": {
	//		"type": "upload_file",
	//		"info_list": {
	//			"data_source_type": "upload_file",
	//			"file_info_list": {
	//				"file_ids": ["89e6269a-86f7-41aa-8546-7c574bf14bdd"]
	//			}
	//		}
	//	},
	//	"indexing_technique": "economy",
	//	"process_rule": {
	//		"rules": {},
	//		"mode": "automatic"
	//	}
	//}

	var body struct {
		IndexingTechnique string `json:"indexing_technique"`
		DataSource        struct {
			Type     string `json:"type"`
			InfoList struct {
				DataSourceType string `json:"data_source_type"`
				FileInfoList   struct {
					FileIds []string `json:"file_ids"`
				} `json:"file_info_list"`
			} `json:"info_list"`
		} `json:"data_source"`
		ProcessRule struct {
			Mode  string   `json:"mode"`
			Rules struct{} `json:"rules"`
		} `json:"process_rule"`
	}

	body.IndexingTechnique = "economy"
	body.DataSource.Type = "upload_file"
	body.DataSource.InfoList.DataSourceType = "upload_file"
	body.DataSource.InfoList.FileInfoList.FileIds = []string{fileId}
	body.ProcessRule.Mode = "automatic"

	statusCode, respBody, respHeader, _ := JSONWithResp("http://localhost/console/api/datasets/"+datasetId+"/documents",
		"POST",
		difyHeaders,
		body,
		time.Duration(time.Second*10))

	fmt.Println(statusCode, respHeader, string(respBody))

	//var myRespBody map[string]interface{}
	//err := json.Unmarshal([]byte(respBody), &myRespBody)
	//if err != nil {
	//	fmt.Println(err)
	//}
	//datasetId = myRespBody["id"].(string)
}

func IsDir(path string) bool {
	s, err := os.Stat(path)
	if err != nil {
		return false
	}
	return s.IsDir()
}
