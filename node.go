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
	"errors"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/utils/str"
)

const (
	defaultNodeIdPrefix = "node"
)

// OperatorRuntime 节点组件实例定义
type OperatorRuntime struct {
	//组件实例
	types.Operator
	//组件配置
	Node *Node
	//规则引擎配置
	EngineConfig types.EngineConfig
}


// node : 静态配置
// node ctx : 初始化完成的配置，同时关联(保存)了静态配置
// node flow ctx : 运行时节点的上下文信息

//CreateOperatorRuntime 初始化 OperatorRuntime
func CreateOperatorRuntime(config types.EngineConfig, node *Node) (*OperatorRuntime, error) {
	operator, err := config.ComponentsRegistry.NewOperator(node.Type)
	if err != nil {
		return &OperatorRuntime{}, err
	} else {
		if node.Configuration == nil {
			node.Configuration = make(types.Configuration)
		}
		if err = operator.Init(config, processGlobalPlaceholders(config, node.Configuration)); err != nil {
			return &OperatorRuntime{}, err
		} else {
			return &OperatorRuntime{
				Operator:     operator,
				Node:         node,
				EngineConfig: config,
			}, nil
		}
	}

}

func (rn *OperatorRuntime) IsDebugMode() bool {
	return rn.Node.DebugMode
}

func (rn *OperatorRuntime) GetOperatorId() types.OperatorId {
	return types.OperatorId{Id: rn.Node.Id, Type: types.NODE}
}

func (rn *OperatorRuntime) ReloadSelf(def []byte) error {
	if ruleNodeCtx, err := rn.EngineConfig.Parser.DecodeRuleNode(rn.EngineConfig, def); err == nil {
		//先销毁
		rn.Destroy()
		//重新加载
		rn.Copy(ruleNodeCtx.(*OperatorRuntime))
		return nil
	} else {
		return err
	}
}

func (rn *OperatorRuntime) ReloadChild(_ types.OperatorId, _ []byte) error {
	return errors.New("not support this func")
}

func (rn *OperatorRuntime) GetOperatorById(_ types.OperatorId) (types.OperatorRuntime, bool) {
	return nil, false
}

func (rn *OperatorRuntime) DSL() []byte {
	v, _ := rn.EngineConfig.Parser.EncodeRuleNode(rn.Node)
	return v
}

// Copy 复制
func (rn *OperatorRuntime) Copy(newCtx *OperatorRuntime) {
	rn.Operator = newCtx.Operator

	rn.Node.AdditionalInfo = newCtx.Node.AdditionalInfo
	rn.Node.Name = newCtx.Node.Name
	rn.Node.Type = newCtx.Node.Type
	rn.Node.DebugMode = newCtx.Node.DebugMode
	rn.Node.Configuration = newCtx.Node.Configuration
}

// 使用全局配置替换节点占位符配置，例如：${global.propertyKey}
func processGlobalPlaceholders(config types.EngineConfig, configuration types.Configuration) types.Configuration {
	if config.Properties.Values() != nil {
		var result = make(types.Configuration)
		for key, value := range configuration {
			if strV, ok := value.(string); ok {
				result[key] = str.SprintfVar(strV, "global.", config.Properties.Values())
			} else {
				result[key] = value
			}
		}
		return result
	}
	return configuration
}
