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

package rulego

import (
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/json"
)

//Chain 规则链定义
type Chain struct {
	//规则链基础信息定义
	Meta Meta `json:"ruleChain"`
	//包含了规则链中节点和连接的信息
	Dag Dag `json:"metadata"`
}

//ParseChain 通过json解析规则链结构体
func ParseChain(cfg []byte) (Chain, error) {
	var chain Chain
	err := json.Unmarshal(cfg, &chain)
	return chain, err
}

//Meta 规则链基础信息定义
type Meta struct {
	//规则链ID
	ID string `json:"id"`
	//扩展字段
	AdditionalInfo map[string]string `json:"additionalInfo"`
	//Name 规则链的名称
	Name string `json:"name"`
	//表示这个节点是否处于调试模式。如果为真，当节点处理消息时，会触发调试回调函数。
	//优先使用子节点的DebugMode配置
	DebugMode bool `json:"debugMode"`
	//Root 表示这个规则链是根规则链还是子规则链。(只做标记使用，非应用在实际逻辑)
	Root bool `json:"root"`
	//Configuration 规则链配置信息
	Configuration types.Config `json:"configuration"`
}

//Dag 规则链元数据定义，包含了规则链中节点和连接的信息
type Dag struct {
	//数据流转的第一个节点，默认:0
	FirstNodeIndex int `json:"firstNodeIndex"`
	//节点组件定义
	//每个对象代表规则链中的一个规则节点
	Nodes []*Node `json:"ops"`
	//连接定义
	//每个对象代表规则链中两个节点之间的连接
	Connections []NodeConnection `json:"cons"`
}

//Node 规则链节点信息定义
type Node struct {
	//节点的唯一标识符，可以是任意字符串
	Id string `json:"id"`
	//扩展字段
	Extend NodeExtend `json:"extend"`
	//节点的类型，决定了节点的逻辑和行为。它应该与规则引擎中注册的节点类型之一匹配。
	Type string `json:"type"`
	//节点的名称，可以是任意字符串
	Name string `json:"name"`
	//表示这个节点是否处于调试模式。如果为真，当节点处理消息时，会触发调试回调函数。
	Debug bool `json:"debug"`
	//包含了节点的配置参数，具体内容取决于节点类型。
	//例如，一个JS过滤器节点可能有一个`jsScript`字段，定义了过滤逻辑，
	//而一个REST API调用节点可能有一个`restEndpointUrlPattern`字段，定义了要调用的URL。
	Config types.Config `json:"configuration"`
}

//ParserNode 通过json解析节点结构体
func ParserNode(cfg []byte) (Node, error) {
	var node Node
	err := json.Unmarshal(cfg, &node)
	return node, err
}

//NodeExtend 用于可视化位置信息(预留字段)
type NodeExtend struct {
	Description string `json:"description"`
	LayoutX     int    `json:"layoutX"`
	LayoutY     int    `json:"layoutY"`
}

//NodeConnection 规则链节点连接定义
//每个对象代表规则链中两个节点之间的连接
type NodeConnection struct {
	//连接的源节点的id，应该与nodes数组中的某个节点id匹配。
	From string `json:"from"`
	//连接的目标节点的id，应该与nodes数组中的某个节点id匹配
	To string `json:"to"`
	//连接的类型，决定了什么时候以及如何把消息从一个节点发送到另一个节点。它应该与源节点类型支持的连接类型之一匹配。
	//例如，一个JS过滤器节点可能支持两种连接类型："True"和"False"，表示消息是否通过或者失败过滤条件。
	Type string `json:"type"`
}

//RuleChainConnection 子规则链连接定义
//每个对象代表规则链中一个节点和一个子规则链之间的连接
type RuleChainConnection struct {
	//连接的源节点的id，应该与nodes数组中的某个节点id匹配。
	FromId string `json:"fromId"`
	//连接的目标子规则链的id，应该与规则引擎中注册的子规则链之一匹配。
	ToId string `json:"toId"`
	//连接的类型，决定了什么时候以及如何把消息从一个节点发送到另一个节点。它应该与源节点类型支持的连接类型之一匹配。
	Type string `json:"type"`
}
