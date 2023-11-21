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

package types

import (
	"context"
	"github.com/rulego/rulego/utils/str"
)

// 关系 节点与节点连接的关系，以下是常用的关系，可以自定义
// relation types
const (
	Success = "Success"
	Failure = "Failure"
	True    = "True"
	False   = "False"
)

// flow direction type
// 流向 消息流入、流出节点方向
const (
	In  = "IN"
	Out = "OUT"
)

// Config 组件配置类型
type Config map[string]interface{}

func (c Config) GetString(key string) string {
	if v, ok := c[key]; ok {
		return str.ToString(v)
	}
	return ""
}

//OperatorType 组件类型：规则节点或者子规则链
type OperatorType int

const (
	NODE OperatorType = iota
	CHAIN
)

//PluginRegistry go plugin 方式提供节点组件接口
//示例：
//package main
//var Plugins MyPlugins// plugin entry point
//type MyPlugins struct{}
//
//func (p *MyPlugins) Init() error {
//	return nil
//}
//func (p *MyPlugins) Components() []types.Operator {
//	return []types.Operator{&UpperNode{}, &TimeNode{}, &FilterNode{}}//一个插件可以提供多个组件
//}
//go build -buildmode=plugin -o plugin.so plugin.go # 编译插件，生成plugin.so文件
//rulego.Registry.RegisterPlugin("test", "./plugin.so")//注册到RuleGo默认注册器9
type PluginRegistry interface {
	//Init 初始化
	Init() error
	//Components 组件列表
	Components() []Operator
}

//Registry 节点组件注册器
type Registry interface {
	//Register 注册组件，如果`node.Type()`已经存在则返回一个`已存在`错误
	Register(op Operator) error
	//RegisterPlugin 通过plugin机制加载外部.so文件注册组件，
	//如果`name`已经存在或者插件提供的组件列表`node.Type()`已经存在则返回一个`已存在`错误
	RegisterPlugin(name string, file string) error
	//Unregister 删除组件或者通过插件名称删除一批组件
	Unregister(componentType string) error
	//NewOperator 通过nodeType创建一个新的node实例
	NewOperator(nodeType string) (Operator, error)
	//GetComponents 获取所有注册组件列表
	GetComponents() map[string]Operator
	//GetComponentForms 获取所有注册组件配置表单，用于可视化配置
	GetComponentForms() ComponentFormList
}

// Operator 规则引擎节点组件接口
//把业务封或者通用逻辑装成组件，然后通过规则链配置方式调用该组件
//实现方式参考`components`包
//然后注册到`RG`默认注册器
//rulego.Registry.Register(&MyNode{})
type Operator interface {
	//New 创建一个组件新实例
	//每个规则链里的规则节点都会创建一个新的实例，数据是独立的
	New() Operator
	//Type 组件类型，类型不能重复。
	//用于规则链，node.type配置，初始化对应的组件
	//建议使用`/`区分命名空间，防止冲突。例如：x/httpClient
	Type() string
	//Init 组件初始化，一般做一些组件参数配置或者客户端初始化操作
	//规则链里的规则节点初始化会调用一次
	Init(configuration Config) error
	//Destroy 销毁，做一些资源释放操作
	Destroy()
	//OnMsg 处理消息，每条流入组件的数据会经过该函数处理
	//ctx:规则引擎处理消息上下文
	//msg:消息
	OnMsg(ctx OperatorContext, msg RuleMsg) error
}

//OperatorRuntime 规则节点实例化上下文
type OperatorRuntime interface {
	Operator
	//IsDebugMode 该节点是否是调试模式
	//True:消息流入和流出该节点，会调用config.OnDebug回调函数，否则不会
	IsDebugMode() bool
	//GetOperatorId 获取组件ID
	GetOperatorId() OperatorId
	//Reload 刷新该组件配置
	Reload(def []byte) error
	//ReloadChild
	//如果是子规则链类型，则刷新该子规则链指定ID组件配置
	//如果是节点类型，则不支持该方法
	ReloadChild(nodeId OperatorId, def []byte) error
	//GetOperatorById
	//如果是子规则链类型，则获取该子规则链指定ID组件配置
	//如果是节点类型，则不支持该方法
	GetOperatorById(nodeId OperatorId) (OperatorRuntime, bool)
	//DSL 返回该节点配置DSL
	DSL() []byte
}

// OperatorContext 规则引擎消息处理上下文接口
//处理把消息流转到下一个或者多个节点逻辑
//根据规则链连接关系查找当前节点的下一个或者多个节点，然后调用对应节点：nextNode.OnMsg(ctx, msg)触发下一个节点的业务逻辑
//另外处理节点OnDebug和OnEnd回调逻辑
type OperatorContext interface {
	//TellSuccess 通知规则引擎处理当前消息处理成功，并把消息通过`Success`关系发送到下一个节点
	TellSuccess(msg RuleMsg)
	//TellFailure 通知规则引擎处理当前消息处理失败，并把消息通过`Failure`关系发送到下一个节点
	TellFailure(msg RuleMsg, err error)
	//TellNext 使用指定的relationTypes，发送消息到下一个节点
	//Send the message to the next node
	TellNext(msg RuleMsg, relationTypes ...string)
	//TellSelf 以指定的延迟（毫秒）向当前规则节点发送消息。
	TellSelf(msg RuleMsg, delayMs int64)
	//TellFlow 执行子规则链，ruleChainId 规则链ID
	//onEndFunc 子规则链链分支执行完的回调，并返回该链执行结果，如果同时触发多个分支链，则会调用多次
	//onAllNodeCompleted 所以节点执行完之后的回调，无结果返回
	//如果找不到规则链，并把消息通过`Failure`关系发送到下一个节点
	TellFlow(msg RuleMsg, ruleChainId string, endFunc func(msg RuleMsg, err error), onAllNodeCompleted func())
	//NewMsg 创建新的消息实例
	NewMsg(msgType string, metaData Metadata, data string) RuleMsg
	//GetSelfId 获取当前节点ID
	GetSelfId() string
	//Configuration 获取规则引擎配置
	Config() Configuration
	//SubmitTack 异步执行任务
	SubmitTack(task func())
	//SetEndFunc 设置当前消息处理结束回调函数
	SetEndFunc(f func(msg RuleMsg, err error)) OperatorContext
	//GetEndFunc 获取当前消息处理结束回调函数
	GetEndFunc() func(msg RuleMsg, err error)
	//SetContext 设置用于不同组件实例共享信号量或者数据的上下文
	SetContext(c context.Context) OperatorContext
	//GetContext 获取用于不同组件实例共享信号量或者数据的上下文
	GetContext() context.Context
	//SetOnAllNodeCompleted 设置所有节点执行完回调
	SetOnAllNodeCompleted(onAllNodeCompleted func())
}

// OperatorContextOption 修改RuleContext选项的函数
type OperatorContextOption func(OperatorContext)

func WithEndFunc(endFunc func(msg RuleMsg, err error)) OperatorContextOption {
	return func(rc OperatorContext) {
		rc.SetEndFunc(endFunc)
	}
}

func WithContext(c context.Context) OperatorContextOption {
	return func(rc OperatorContext) {
		rc.SetContext(c)
	}
}

func WithOnAllNodeCompleted(onAllNodeCompleted func()) OperatorContextOption {
	return func(rc OperatorContext) {
		rc.SetOnAllNodeCompleted(onAllNodeCompleted)
	}
}

//JsEngine JavaScript脚本引擎
type JsEngine interface {
	//Execute 执行js脚本指定函数，js脚本在JsEngine实例化的时候进行初始化
	//functionName 执行的函数名
	//argumentList 函数参数列表
	Execute(functionName string, argumentList ...interface{}) (interface{}, error)
	//Stop 释放js引擎资源
	Stop()
}

//Pool 协程池
type Pool interface {
	//Submit 往协程池提交一个任务
	//如果协程池满返回错误
	Submit(task func()) error
	//Release 释放
	Release()
}

//EmptyRuleNodeId 空节点ID
var EmptyRuleNodeId = OperatorId{}

// OperatorId 组件ID类型定义
type OperatorId struct {
	//节点ID
	Id string
	//节点类型，节点/子规则链
	Type OperatorType
}

//OperatorConnection 节点与节点之间关系
type OperatorConnection struct {
	//入组件ID
	From OperatorId
	//出组件ID
	To OperatorId
	//关系 如：True、False、Success、Failure 或者其他自定义关系
	Type string
}
