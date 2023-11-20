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
	"context"
	"fmt"
	"github.com/rulego/rulego/api/types"
	"sync"
)

type RelationCache struct {
	//入接点
	inNodeId types.OperatorId
	//与出节点连接关系
	connType string
}

// ChainCtx 规则链实例定义
//初始化所有节点
//记录规则链，所有节点路由关系
type ChainCtx struct {
	//节点ID
	Id types.OperatorId
	//规则链定义
	Chain *Chain
	//规则引擎配置
	EngineConfig types.EngineConfig
	//是否已经初始化
	initialized bool
	//组件库
	components types.Registry
	//节点ID列表
	nodeIds []types.OperatorId
	//组件列表
	operators map[types.OperatorId]types.OperatorRuntime
	//组件路由关系
	connections   map[types.OperatorId][]types.OperatorConnection
	nodeCtxRoutes map[types.OperatorId][]types.OperatorRuntime
	//通过入节点查询指定关系出节点列表缓存
	relationCache map[RelationCache][]types.OperatorRuntime

	//根上下文
	rootOperatorCtx types.OperatorContext

	//子规则链池
	ruleChainPool *RuleGo
	sync.RWMutex
}

//CreateChainCtx 初始化RuleChainCtx
func CreateChainCtx(engineConfig types.EngineConfig, chain *Chain) (*ChainCtx, error) {

	var chainCtx = &ChainCtx{
		EngineConfig:  engineConfig,
		Chain:         chain,
		operators:     make(map[types.OperatorId]types.OperatorRuntime),
		connections:   make(map[types.OperatorId][]types.OperatorConnection),
		relationCache: make(map[RelationCache][]types.OperatorRuntime),
		components:    engineConfig.ComponentsRegistry,
		initialized:   true,
	}

	if chain.RuleChain.ID != "" {
		chainCtx.Id = types.OperatorId{Id: chain.RuleChain.ID, Type: types.CHAIN}
	}
	nodeLen := len(chain.Meta.Nodes)
	chainCtx.nodeIds = make([]types.OperatorId, nodeLen)

	//加载所有节点信息
	for index, node := range chain.Meta.Nodes {
		if node.Id == "" {
			node.Id = fmt.Sprintf(defaultNodeIdPrefix+"%d", index)
		}
		id := types.OperatorId{Id: node.Id, Type: types.NODE}
		chainCtx.nodeIds[index] = id 		// 保存 index => id

		op, err := CreateOperatorRuntime(engineConfig, node)
		if err != nil {
			return nil, err
		}
		chainCtx.operators[id] = op // 保存 id => ctx
	}

	//加载节点关系信息
	for _, conn := range chain.Meta.Connections {
		from := types.OperatorId{Id: conn.FromId, Type: types.NODE}
		to := types.OperatorId{Id: conn.ToId, Type: types.NODE}
		relation := types.OperatorConnection{
			From: from,
			To:   to,
			Type: conn.Type,
		}

		connections, ok := chainCtx.connections[from]
		if ok {
			connections = append(connections, relation)
		} else {
			connections = []types.OperatorConnection{relation}
		}
		chainCtx.connections[from] = connections
	}

	//加载子规则链
	for _, item := range chain.Meta.RuleChainConnections {
		from := types.OperatorId{Id: item.FromId, Type: types.NODE}
		to := types.OperatorId{Id: item.ToId, Type: types.CHAIN}
		relation := types.OperatorConnection{
			From: from,
			To:   to,
			Type: item.Type,
		}

		connections, ok := chainCtx.connections[from]
		if ok {
			connections = append(connections, relation)
		} else {
			connections = []types.OperatorConnection{relation}
		}
		chainCtx.connections[from] = connections
	}

	if root, ok := chainCtx.GetRootOperator(); ok {
		chainCtx.rootOperatorCtx = NewOperatorContext(
			context.TODO(),
			chainCtx.EngineConfig,
			chainCtx,
			nil,
			root,
			engineConfig.Pool,
			nil,
			nil)
	}

	return chainCtx, nil
}

func (rc *ChainCtx) GetOperatorById(id types.OperatorId) (types.OperatorRuntime, bool) {
	rc.RLock()
	defer rc.RUnlock()
	if id.Type == types.CHAIN {
		//子规则链通过规则链池查找
		if subRuleEngine, ok := rc.GetRuleChainPool().Get(id.Id); ok && subRuleEngine.chainCtx != nil {
			return subRuleEngine.chainCtx, true
		} else {
			return nil, false
		}
	} else {
		ruleNodeCtx, ok := rc.operators[id]
		return ruleNodeCtx, ok
	}

}

func (rc *ChainCtx) GetOperatorByIndex(index int) (types.OperatorRuntime, bool) {
	if index >= len(rc.nodeIds) {
		return &OperatorRuntime{}, false
	}
	return rc.GetOperatorById(rc.nodeIds[index])
}

//GetRootOperator 获取第一个节点，消息从该节点开始流转。默认是index=0的节点
func (rc *ChainCtx) GetRootOperator() (types.OperatorRuntime, bool) {
	return rc.GetOperatorByIndex(rc.Chain.Meta.FirstNodeIndex)
}

func (rc *ChainCtx) GetNodeConnection(id types.OperatorId) ([]types.OperatorConnection, bool) {
	rc.RLock()
	defer rc.RUnlock()
	connections, ok := rc.connections[id]
	return connections, ok
}

// GetToOperators 获取当前节点指定关系的子节点
func (rc *ChainCtx) GetToOperators(id types.OperatorId, connType string) ([]types.OperatorRuntime, bool) {
	cacheKey := RelationCache{inNodeId: id, connType: connType}
	rc.RLock()
	//get from cache
	nodeCtxList, ok := rc.relationCache[cacheKey]
	rc.RUnlock()
	if ok {
		return nodeCtxList, nodeCtxList != nil
	}

	//get from the Routes
	var targets []types.OperatorRuntime
	connections, ok := rc.GetNodeConnection(id)
	hasTargetConn := false
	if ok {
		for _, connection := range connections {
			if connection.Type == connType {
				if toNode, ok := rc.GetOperatorById(connection.To); ok {
					targets = append(targets, toNode)
					hasTargetConn = true
				}
			}
		}
	}
	rc.Lock()
	//add to the cache
	rc.relationCache[cacheKey] = targets
	rc.Unlock()
	return targets, hasTargetConn
}

// Type 组件类型
func (rc *ChainCtx) Type() string {
	return "ruleChain"
}

func (rc *ChainCtx) New() types.Operator {
	panic("not support this func")
}

// Init 初始化
func (rc *ChainCtx) Init(_ types.EngineConfig, configuration types.Configuration) error {
	if rootRuleChainDef, ok := configuration["selfDefinition"]; ok {
		if v, ok := rootRuleChainDef.(*Chain); ok {
			if ruleChainCtx, err := CreateChainCtx(rc.EngineConfig, v); err == nil {
				rc.Copy(ruleChainCtx)
			} else {
				return err
			}
		}
	}
	return nil
	//return errors.New("not support this func")
}

// OnMsg 处理消息
func (rc *ChainCtx) OnMsg(ctx types.OperatorContext, msg types.RuleMsg) error {
	ctx.TellFlow(msg, rc.Id.Id, nil, nil)
	return nil
}

func (rc *ChainCtx) Destroy() {
	rc.RLock()
	defer rc.RUnlock()
	for _, v := range rc.operators {
		temp := v
		temp.Destroy()
	}
}

func (rc *ChainCtx) IsDebugMode() bool {
	return rc.Chain.RuleChain.DebugMode
}

func (rc *ChainCtx) GetOperatorId() types.OperatorId {
	return rc.Id
}

func (rc *ChainCtx) ReloadSelf(def []byte) error {
	if ctx, err := rc.EngineConfig.Parser.DecodeRuleChain(rc.EngineConfig, def); err == nil {
		rc.Destroy()
		rc.Copy(ctx.(*ChainCtx))

	} else {
		return err
	}
	return nil
}

func (rc *ChainCtx) ReloadChild(ruleNodeId types.OperatorId, def []byte) error {
	if node, ok := rc.GetOperatorById(ruleNodeId); ok {
		//更新子节点
		if err := node.ReloadSelf(def); err != nil {
			return err
		}
	}
	return nil
}

func (rc *ChainCtx) DSL() []byte {
	v, _ := rc.EngineConfig.Parser.EncodeRuleChain(rc.Chain)
	return v
}

// Copy 复制
func (rc *ChainCtx) Copy(newCtx *ChainCtx) {
	rc.Lock()
	defer rc.Unlock()
	rc.Id = newCtx.Id
	rc.EngineConfig = newCtx.EngineConfig
	rc.initialized = newCtx.initialized
	rc.components = newCtx.components
	rc.Chain = newCtx.Chain
	rc.nodeIds = newCtx.nodeIds
	rc.operators = newCtx.operators
	rc.connections = newCtx.connections
	rc.rootOperatorCtx = newCtx.rootOperatorCtx
	rc.ruleChainPool = newCtx.ruleChainPool
	//清除缓存
	rc.relationCache = make(map[RelationCache][]types.OperatorRuntime)
}

//SetRuleChainPool 设置子规则链池
func (rc *ChainCtx) SetRuleChainPool(ruleChainPool *RuleGo) {
	rc.ruleChainPool = ruleChainPool
}

//GetRuleChainPool 获取子规则链池
func (rc *ChainCtx) GetRuleChainPool() *RuleGo {
	if rc.ruleChainPool == nil {
		return DefaultRuleGo
	} else {
		return rc.ruleChainPool
	}
}
