package model

import (
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/json"
)

//Node 规则链节点信息定义
type Node struct {
	//节点的唯一标识符，可以是任意字符串
	Id string `json:"id"`
	//扩展字段
	AdditionalInfo NodeAdditionalInfo `json:"additionalInfo"`
	//节点的类型，决定了节点的逻辑和行为。它应该与规则引擎中注册的节点类型之一匹配。
	Type string `json:"type"`
	//节点的名称，可以是任意字符串
	Name string `json:"name"`
	//表示这个节点是否处于调试模式。如果为真，当节点处理消息时，会触发调试回调函数。
	DebugMode bool `json:"debugMode"`
	//包含了节点的配置参数，具体内容取决于节点类型。
	//例如，一个JS过滤器节点可能有一个`jsScript`字段，定义了过滤逻辑，
	//而一个REST API调用节点可能有一个`restEndpointUrlPattern`字段，定义了要调用的URL。
	Configuration types.Config `json:"configuration"`
}

//NodeAdditionalInfo 用于可视化位置信息(预留字段)
type NodeAdditionalInfo struct {
	Description string `json:"description"`
	LayoutX     int    `json:"layoutX"`
	LayoutY     int    `json:"layoutY"`
}

//ParserRuleNode 通过json解析节点结构体
func ParserRuleNode(rootRuleChain []byte) (Node, error) {
	var def Node
	err := json.Unmarshal(rootRuleChain, &def)
	return def, err
}
