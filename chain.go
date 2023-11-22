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
	"fmt"
	"github.com/rulego/rulego/api/types"
	"sync"
)

// ChainCtx 规则链实例定义
//初始化所有节点
//记录规则链，所有节点路由关系
type ChainCtx struct {
	Id     types.OperatorId
	Chain  *Chain
	Engine *Engine

	sync.RWMutex
	opIds []types.OperatorId
	ops   map[types.OperatorId]types.OperatorRuntime
	cons  map[types.OperatorId][]types.OperatorConnection
}

//NewChainCtx 初始化RuleChainCtx
func NewChainCtx(engine *Engine, chain *Chain) (*ChainCtx, error) {
	var chainCtx = &ChainCtx{
		Id:     types.OperatorId{Id: chain.Meta.ID, Type: types.CHAIN},
		Engine: engine,
		Chain:  chain,
		ops:    make(map[types.OperatorId]types.OperatorRuntime),
		cons:   make(map[types.OperatorId][]types.OperatorConnection),
		opIds:  make([]types.OperatorId, len(chain.Dag.Nodes)),
	}
	//加载所有节点信息
	for idx, node := range chain.Dag.Nodes {
		if node.Id == "" {
			node.Id = fmt.Sprintf(defaultNodeIdPrefix+"%d", idx)
		}
		id := types.OperatorId{Id: node.Id, Type: types.NODE}
		chainCtx.opIds[idx] = id // 保存 idx => id
		op, err := NewOperatorRuntime(engine, node)
		if err != nil {
			return nil, err
		}
		chainCtx.ops[id] = op // 保存 id => ctx
	}

	//加载节点关系信息
	for _, conn := range chain.Dag.Connections {
		from := types.OperatorId{Id: conn.From, Type: types.NODE}
		to := types.OperatorId{Id: conn.To, Type: types.NODE}
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
	return chainCtx, nil
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
	return ops, hasConn
}

// CopyFrom 复制
func (rc *ChainCtx) CopyFrom(newCtx *ChainCtx) {
	rc.Lock()
	defer rc.Unlock()
	rc.Id = newCtx.Id
	rc.Engine = newCtx.Engine
	rc.Chain = newCtx.Chain
	rc.opIds = newCtx.opIds
	rc.ops = newCtx.ops
	rc.cons = newCtx.cons
}

/// OperatorRuntime ///

func (rc *ChainCtx) IsDebugMode() bool {
	return rc.Chain.Meta.DebugMode
}

func (rc *ChainCtx) GetOperatorId() types.OperatorId {
	return rc.Id
}

func (rc *ChainCtx) Reload(cfg []byte) error {
	chain, err := ParseChain(cfg)
	if err != nil {
		return err
	}
	chainCtx, err := NewChainCtx(rc.Engine, &chain)
	if err != nil {
		return err
	}
	rc.Destroy()
	rc.CopyFrom(chainCtx)
	return nil
}

func (rc *ChainCtx) ReloadChild(id types.OperatorId, cfg []byte) error {
	op, ok := rc.GetOperatorById(id)
	if !ok {
		return nil
	}
	if err := op.Reload(cfg); err != nil {
		return err
	}
	return nil
}

func (rc *ChainCtx) GetOperatorById(id types.OperatorId) (types.OperatorRuntime, bool) {
	rc.RLock()
	defer rc.RUnlock()
	if id.Type == types.CHAIN {
		chainCtx := rc.Engine.GetChain(id.Id)
		if chainCtx == nil {
			return nil, false
		}
		return chainCtx, true
	}
	op, ok := rc.ops[id]
	return op, ok
}

func (rc *ChainCtx) DSL() []byte {
	//v, _ := rc.Config.Parser.EncodeChain(rc.Chain)
	//return v
	return nil
}

/// types.Operator ///

func (rc *ChainCtx) New() types.Operator {
	panic("not support this func")
}

// Type 组件类型
func (rc *ChainCtx) Type() string {
	return "ruleChain"
}

// Init 初始化
func (rc *ChainCtx) Init(config types.Config) error {
	return nil
}

//// OnMsg 处理消息
//func (rc *ChainCtx) OnMsg(ctx types.OperatorContext, msg types.RuleMsg) error {
//	ctx.TellFlow(msg, rc.Id.Id, nil, nil)
//	return nil
//}

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
