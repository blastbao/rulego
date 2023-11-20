package model

import (
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/json"
)

//Chain 规则链定义
type Chain struct {
	//规则链基础信息定义
	RuleChain RuleChainBaseInfo `json:"ruleChain"`
	//包含了规则链中节点和连接的信息
	Meta ChainMeta `json:"metadata"`
}

//ParserChain 通过json解析规则链结构体
func ParserChain(rootRuleChain []byte) (Chain, error) {
	var def Chain
	err := json.Unmarshal(rootRuleChain, &def)
	return def, err
}

//RuleChainBaseInfo 规则链基础信息定义
type RuleChainBaseInfo struct {
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
	Configuration types.Configuration `json:"configuration"`
}

//ChainMeta 规则链元数据定义，包含了规则链中节点和连接的信息
type ChainMeta struct {
	//数据流转的第一个节点，默认:0
	FirstNodeIndex int `json:"firstNodeIndex"`
	//节点组件定义
	//每个对象代表规则链中的一个规则节点
	Nodes []*Node `json:"operators"`
	//连接定义
	//每个对象代表规则链中两个节点之间的连接
	Connections []NodeConnection `json:"connections"`

	//Deprecated
	//使用 Flow Node代替
	//子规则链链接
	//每个对象代表规则链中一个节点和一个子规则链之间的连接
	RuleChainConnections []RuleChainConnection `json:"ruleChainConnections,omitempty"`
}

//NodeConnection 规则链节点连接定义
//每个对象代表规则链中两个节点之间的连接
type NodeConnection struct {
	//连接的源节点的id，应该与nodes数组中的某个节点id匹配。
	FromId string `json:"fromId"`
	//连接的目标节点的id，应该与nodes数组中的某个节点id匹配
	ToId string `json:"toId"`
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
