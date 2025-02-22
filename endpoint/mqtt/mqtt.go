/*
 * Copyright 2023 The RG Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mqtt

import (
	"context"
	"errors"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/mqtt"
	"github.com/rulego/rulego/endpoint"
	"github.com/rulego/rulego/utils/maps"
	"net/textproto"
	"strconv"
	"time"
)

//Type 组件类型
const Type = "mqtt"

//Endpoint 别名
type Endpoint = Mqtt

//注册组件
func init() {
	_ = endpoint.Registry.Register(&Endpoint{})
}

//RequestMessage http请求消息
type RequestMessage struct {
	request paho.Message
	msg     *types.RuleMsg
	err     error
}

func (r *RequestMessage) Body() []byte {
	return r.request.Payload()
}
func (r *RequestMessage) Headers() textproto.MIMEHeader {
	header := make(map[string][]string)
	header["topic"] = []string{r.request.Topic()}
	return header
}

func (r *RequestMessage) From() string {
	return r.request.Topic()
}

func (r *RequestMessage) GetParam(key string) string {
	return ""
}

func (r *RequestMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}

func (r *RequestMessage) GetMsg() *types.RuleMsg {
	if r.msg == nil {
		//默认指定是JSON格式，如果不是该类型，请在process函数中修改
		ruleMsg := types.NewMsg(0, r.From(), types.JSON, types.NewMetadata(), string(r.Body()))

		ruleMsg.Metadata.PutValue("topic", r.From())

		r.msg = &ruleMsg
	}
	return r.msg
}

func (r *RequestMessage) SetStatusCode(statusCode int) {
}

func (r *RequestMessage) SetBody(body []byte) {
}

func (r *RequestMessage) SetError(err error) {
	r.err = err
}

func (r *RequestMessage) GetError() error {
	return r.err
}

func (r *RequestMessage) Request() paho.Message {
	return r.request
}

//ResponseMessage http响应消息
type ResponseMessage struct {
	request  paho.Message
	response paho.Client
	body     []byte
	msg      *types.RuleMsg
	headers  textproto.MIMEHeader
	err      error
}

func (r *ResponseMessage) Body() []byte {
	return r.body
}

func (r *ResponseMessage) Headers() textproto.MIMEHeader {
	if r.headers == nil {
		r.headers = make(map[string][]string)
	}
	return r.headers
}

func (r *ResponseMessage) From() string {
	return r.request.Topic()
}

func (r *ResponseMessage) GetParam(key string) string {
	return ""
}

func (r *ResponseMessage) SetMsg(msg *types.RuleMsg) {
	r.msg = msg
}
func (r *ResponseMessage) GetMsg() *types.RuleMsg {
	return r.msg
}

func (r *ResponseMessage) SetStatusCode(statusCode int) {
}

func (r *ResponseMessage) SetBody(body []byte) {
	r.body = body

	topic := r.Headers().Get("topic")
	if topic != "" {
		qosStr := r.Headers().Get("qos")
		qos := byte(0)
		if qosStr != "" {
			qosInt, _ := strconv.Atoi(qosStr)
			qos = byte(qosInt)
		}
		r.response.Publish(topic, qos, false, r.body)
	}
}

func (r *ResponseMessage) SetError(err error) {
	r.err = err
}

func (r *ResponseMessage) GetError() error {
	return r.err
}

func (r *ResponseMessage) Response() paho.Client {
	return r.response
}

//Mqtt MQTT 接收端端点
type Mqtt struct {
	endpoint.BaseEndpoint
	RuleConfig types.Configuration
	Config     mqtt.Config
	client     *mqtt.Client
}

//Type 组件类型
func (m *Mqtt) Type() string {
	return Type
}

func (m *Mqtt) New() types.Operator {
	return &Mqtt{}
}

//Init 初始化
func (m *Mqtt) Init(configuration types.Config) error {
	err := maps.Map2Struct(configuration, &m.Config)
	m.RuleConfig = ruleConfig
	return err
}

//Destroy 销毁
func (m *Mqtt) Destroy() {
	_ = m.Close()
}

func (m *Mqtt) Close() error {
	if nil != m.client {
		return m.client.Close()
	}
	return nil
}

func (m *Mqtt) Id() string {
	return m.Config.Server
}

func (m *Mqtt) AddRouter(router *endpoint.Router, params ...interface{}) (string, error) {
	if router == nil {
		return "", errors.New("router can not nil")
	}
	m.saveRouter(router)
	//服务已经启动
	if m.client != nil {
		if form := router.GetFrom(); form != nil {
			m.client.RegisterHandler(mqtt.Handler{
				Topic:  form.ToString(),
				Qos:    m.Config.QOS,
				Handle: m.handler(router),
			})
		}
	}
	return router.GetFrom().From, nil
}

func (m *Mqtt) RemoveRouter(routerId string, params ...interface{}) error {
	m.deleteRouter(routerId)
	return m.client.UnregisterHandler(routerId)
}

func (m *Mqtt) Start() error {
	if m.client == nil {
		ctx, cancel := context.WithTimeout(context.TODO(), 16*time.Second)
		defer cancel()
		if client, err := mqtt.NewClient(ctx, m.Config); err != nil {
			return err
		} else {
			m.client = client
			for _, router := range m.RouterStorage {

				if form := router.GetFrom(); form != nil {
					m.client.RegisterHandler(mqtt.Handler{
						Topic:  form.ToString(),
						Qos:    m.Config.QOS,
						Handle: m.handler(router),
					})
				}
			}
			return nil
		}
	}
	return nil
}

//存储路由
func (m *Mqtt) saveRouter(routers ...*endpoint.Router) {
	m.Lock()
	defer m.Unlock()
	if m.RouterStorage == nil {
		m.RouterStorage = make(map[string]*endpoint.Router)
	}
	for _, item := range routers {
		m.RouterStorage[item.FromToString()] = item
	}
}

//从存储器中删除路由
func (m *Mqtt) deleteRouter(from string) {
	m.Lock()
	defer m.Unlock()
	if m.RouterStorage != nil {
		delete(m.RouterStorage, from)
	}
}

func (m *Mqtt) handler(router *endpoint.Router) func(c paho.Client, data paho.Message) {
	return func(c paho.Client, data paho.Message) {
		defer func() {
			//捕捉异常
			if e := recover(); e != nil {
				m.Printf("rest handler err :%v", e)
			}
		}()
		exchange := &endpoint.Exchange{
			In: &RequestMessage{
				request: data,
			},
			Out: &ResponseMessage{
				request:  data,
				response: c,
			}}

		m.DoProcess(router, exchange)
	}
}

func (m *Mqtt) Printf(format string, v ...interface{}) {
	if m.RuleConfig.Logger != nil {
		m.RuleConfig.Logger.Printf(format, v...)
	}
}
