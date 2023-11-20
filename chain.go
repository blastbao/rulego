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

type ConnectionCacheKey struct {
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
	opIds []types.OperatorId
	//组件列表
	ops map[types.OperatorId]types.OperatorRuntime
	//组件路由关系
	cons   map[types.OperatorId][]types.OperatorConnection
	routes map[types.OperatorId][]types.OperatorRuntime

	//通过入节点查询指定关系出节点列表缓存
	consCache map[ConnectionCacheKey][]types.OperatorRuntime

	//根上下文
	rootOperatorCtx types.OperatorContext

	//子规则链池
	ruleChainPool *RuleGo
	sync.RWMutex
}

//NewChainCtx 初始化RuleChainCtx
func NewChainCtx(engineConfig types.EngineConfig, chain *Chain) (*ChainCtx, error) {

	var chainCtx = &ChainCtx{
		EngineConfig: engineConfig,
		Chain:        chain,
		ops:          make(map[types.OperatorId]types.OperatorRuntime),
		cons:         make(map[types.OperatorId][]types.OperatorConnection),
		consCache:    make(map[ConnectionCacheKey][]types.OperatorRuntime),
		components:   engineConfig.ComponentsRegistry,
		initialized:  true,
	}

	if chain.Meta.ID != "" {
		chainCtx.Id = types.OperatorId{Id: chain.Meta.ID, Type: types.CHAIN}
	}

	nodeLen := len(chain.Dag.Nodes)
	chainCtx.opIds = make([]types.OperatorId, nodeLen)

	//加载所有节点信息
	for idx, node := range chain.Dag.Nodes {
		if node.Id == "" {
			node.Id = fmt.Sprintf(defaultNodeIdPrefix+"%d", idx)
		}
		id := types.OperatorId{Id: node.Id, Type: types.NODE}
		chainCtx.opIds[idx] = id // 保存 idx => id

		op, err := NewOperatorRuntime(engineConfig, node)
		if err != nil {
			return nil, err
		}
		chainCtx.ops[id] = op // 保存 id => ctx
	}

	//加载节点关系信息
	for _, conn := range chain.Dag.Connections {
		from := types.OperatorId{Id: conn.FromId, Type: types.NODE}
		to := types.OperatorId{Id: conn.ToId, Type: types.NODE}
		con := types.OperatorConnection{
			From: from,
			To:   to,
			Type: conn.Type,
		}

		cons, ok := chainCtx.cons[from]
		if ok {
			cons = append(cons, con)
		} else {
			cons = []types.OperatorConnection{con}
		}
		chainCtx.cons[from] = cons
	}

	//加载子规则链
	for _, item := range chain.Dag.RuleChainConnections {
		from := types.OperatorId{Id: item.FromId, Type: types.NODE}
		to := types.OperatorId{Id: item.ToId, Type: types.CHAIN}
		relation := types.OperatorConnection{
			From: from,
			To:   to,
			Type: item.Type,
		}

		connections, ok := chainCtx.cons[from]
		if ok {
			connections = append(connections, relation)
		} else {
			connections = []types.OperatorConnection{relation}
		}
		chainCtx.cons[from] = connections
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
		ruleNodeCtx, ok := rc.ops[id]
		return ruleNodeCtx, ok
	}

}

func (rc *ChainCtx) GetOperatorByIndex(index int) (types.OperatorRuntime, bool) {
	if index >= len(rc.opIds) {
		return &OperatorRuntime{}, false
	}
	return rc.GetOperatorById(rc.opIds[index])
}

//GetRootOperator 获取第一个节点，消息从该节点开始流转。默认是index=0的节点
func (rc *ChainCtx) GetRootOperator() (types.OperatorRuntime, bool) {
	return rc.GetOperatorByIndex(rc.Chain.Dag.FirstNodeIndex)
}

func (rc *ChainCtx) GetOperatorCons(id types.OperatorId) ([]types.OperatorConnection, bool) {
	rc.RLock()
	defer rc.RUnlock()
	cons, ok := rc.cons[id]
	return cons, ok
}

// GetToOperators 获取当前节点指定关系的子节点
func (rc *ChainCtx) GetToOperators(id types.OperatorId, connType string) ([]types.OperatorRuntime, bool) {
	// get from cache
	cacheKey := ConnectionCacheKey{inNodeId: id, connType: connType}
	rc.RLock()
	cachedCons, ok := rc.consCache[cacheKey]
	rc.RUnlock()
	if ok {
		return cachedCons, cachedCons != nil
	}

	// get from the routes
	var ops []types.OperatorRuntime
	cons, ok := rc.GetOperatorCons(id)
	hasConn := false
	if ok {
		for _, con := range cons {
			if con.Type == connType {
				if op, ok := rc.GetOperatorById(con.To); ok {
					ops = append(ops, op)
					hasConn = true
				}
			}
		}
	}

	//add to the cache
	rc.Lock()
	rc.consCache[cacheKey] = ops
	rc.Unlock()
	return ops, hasConn
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
			if ruleChainCtx, err := NewChainCtx(rc.EngineConfig, v); err == nil {
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
	for _, op := range rc.ops {
		op := op
		op.Destroy()
	}
}

func (rc *ChainCtx) IsDebugMode() bool {
	return rc.Chain.Meta.DebugMode
}

func (rc *ChainCtx) GetOperatorId() types.OperatorId {
	return rc.Id
}

func (rc *ChainCtx) ReloadSelf(cfg []byte) error {
	if ctx, err := rc.EngineConfig.Parser.DecodeChain(rc.EngineConfig, cfg); err == nil {
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
	v, _ := rc.EngineConfig.Parser.EncodeChain(rc.Chain)
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
	rc.opIds = newCtx.opIds
	rc.ops = newCtx.ops
	rc.cons = newCtx.cons
	rc.rootOperatorCtx = newCtx.rootOperatorCtx
	rc.ruleChainPool = newCtx.ruleChainPool
	//清除缓存
	rc.consCache = make(map[ConnectionCacheKey][]types.OperatorRuntime)
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
