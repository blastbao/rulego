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
	Engine types.Configuration
}


// node : 静态配置
// node ctx : 初始化完成的配置，同时关联(保存)了静态配置
// node flow ctx : 运行时节点的上下文信息

//NewOperatorRuntime 初始化 OperatorRuntime
func NewOperatorRuntime(engine types.Configuration, node *Node) (*OperatorRuntime, error) {
	operator, err := engine.Registry.NewOperator(node.Type)
	if err != nil {
		return &OperatorRuntime{}, err
	} else {
		if node.Config == nil {
			node.Config = make(types.Config)
		}
		if err = operator.Init(engine, processGlobalPlaceholders(engine, node.Config)); err != nil {
			return &OperatorRuntime{}, err
		} else {
			return &OperatorRuntime{
				Operator: operator,
				Node:     node,
				Engine:   engine,
			}, nil
		}
	}
}

func (or *OperatorRuntime) IsDebugMode() bool {
	return or.Node.Debug
}

func (or *OperatorRuntime) GetOperatorId() types.OperatorId {
	return types.OperatorId{Id: or.Node.Id, Type: types.NODE}
}

func (or *OperatorRuntime) ReloadSelf(cfg []byte) error {
	ruleNodeCtx, err := or.Engine.Parser.DecodeNode(or.Engine, cfg)
	if err != nil {
		return err
	}
	//先销毁
	or.Destroy()
	//重新加载
	or.Copy(ruleNodeCtx.(*OperatorRuntime))
	return nil
}

func (or *OperatorRuntime) ReloadChild(_ types.OperatorId, _ []byte) error {
	return errors.New("not support this func")
}

func (or *OperatorRuntime) GetOperatorById(_ types.OperatorId) (types.OperatorRuntime, bool) {
	return nil, false
}

func (or *OperatorRuntime) DSL() []byte {
	v, _ := or.Engine.Parser.EncodeNode(or.Node)
	return v
}

// Copy 复制
func (or *OperatorRuntime) Copy(newCtx *OperatorRuntime) {
	or.Operator = newCtx.Operator

	or.Node.Extend = newCtx.Node.Extend
	or.Node.Name = newCtx.Node.Name
	or.Node.Type = newCtx.Node.Type
	or.Node.Debug = newCtx.Node.Debug
	or.Node.Config = newCtx.Node.Config
}

// 使用全局配置替换节点占位符配置，例如：${global.propertyKey}
func processGlobalPlaceholders(config types.Configuration, configuration types.Config) types.Config {
	if config.Properties.Values() != nil {
		var result = make(types.Config)
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
