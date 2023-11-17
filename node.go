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

// NodeCtx 节点组件实例定义
type NodeCtx struct {
	//组件实例
	types.INode
	//组件配置
	NodeCfg *RuleNode
	//规则引擎配置
	EngineConfig types.EngineConfig
}


// node : 静态配置
// node ctx : 初始化完成的配置，同时关联(保存)了静态配置
// node flow ctx : 运行时节点的上下文信息

//CreateNodeCtx 初始化 NodeCtx
func CreateNodeCtx(config types.EngineConfig, nodeCfg *RuleNode) (*NodeCtx, error) {
	node, err := config.ComponentsRegistry.NewNode(nodeCfg.Type)
	if err != nil {
		return &NodeCtx{}, err
	} else {
		if nodeCfg.Configuration == nil {
			nodeCfg.Configuration = make(types.Configuration)
		}
		if err = node.Init(config, processGlobalPlaceholders(config, nodeCfg.Configuration)); err != nil {
			return &NodeCtx{}, err
		} else {
			return &NodeCtx{
				INode:        node,
				NodeCfg:      nodeCfg,
				EngineConfig: config,
			}, nil
		}
	}

}

func (rn *NodeCtx) IsDebugMode() bool {
	return rn.NodeCfg.DebugMode
}

func (rn *NodeCtx) GetNodeId() types.NodeId {
	return types.NodeId{Id: rn.NodeCfg.Id, Type: types.NODE}
}

func (rn *NodeCtx) ReloadSelf(def []byte) error {
	if ruleNodeCtx, err := rn.EngineConfig.Parser.DecodeRuleNode(rn.EngineConfig, def); err == nil {
		//先销毁
		rn.Destroy()
		//重新加载
		rn.Copy(ruleNodeCtx.(*NodeCtx))
		return nil
	} else {
		return err
	}
}

func (rn *NodeCtx) ReloadChild(_ types.NodeId, _ []byte) error {
	return errors.New("not support this func")
}

func (rn *NodeCtx) GetNodeCtxById(_ types.NodeId) (types.NodeCtx, bool) {
	return nil, false
}

func (rn *NodeCtx) DSL() []byte {
	v, _ := rn.EngineConfig.Parser.EncodeRuleNode(rn.NodeCfg)
	return v
}

// Copy 复制
func (rn *NodeCtx) Copy(newCtx *NodeCtx) {
	rn.INode = newCtx.INode

	rn.NodeCfg.AdditionalInfo = newCtx.NodeCfg.AdditionalInfo
	rn.NodeCfg.Name = newCtx.NodeCfg.Name
	rn.NodeCfg.Type = newCtx.NodeCfg.Type
	rn.NodeCfg.DebugMode = newCtx.NodeCfg.DebugMode
	rn.NodeCfg.Configuration = newCtx.NodeCfg.Configuration
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
