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
)

// RuleEngine 规则引擎
//每个规则引擎实例只有一个根规则链，如果没设置规则链则无法处理数据
type RuleEngine struct {
	//规则引擎实例标识
	Id string
	//配置
	Config types.EngineConfig
	//子规则链池
	RuleChainPool *RuleGo
	//根规则链
	chainCtx *ChainCtx
}

func newRuleEngine(id string, def []byte, opts ...RuleEngineOption) (*RuleEngine, error) {
	if len(def) == 0 {
		return nil, errors.New("def can not nil")
	}
	// Create a new RuleEngine with the Id
	ruleEngine := &RuleEngine{
		Id:            id,
		Config:        NewConfig(),
		RuleChainPool: DefaultRuleGo,
	}
	err := ruleEngine.ReloadSelf(def, opts...)
	if err == nil && ruleEngine.chainCtx != nil {
		if id != "" {
			ruleEngine.chainCtx.Id = types.OperatorId{Id: id, Type: types.CHAIN}
		} else {
			//使用规则链ID
			ruleEngine.Id = ruleEngine.chainCtx.Id.Id
		}
	}

	return ruleEngine, err
}

// ReloadSelf 重新加载规则链
func (e *RuleEngine) ReloadSelf(def []byte, opts ...RuleEngineOption) error {
	// Apply the options to the RuleEngine.
	for _, opt := range opts {
		_ = opt(e)
	}
	//初始化
	if ctx, err := e.Config.Parser.DecodeChain(e.Config, def); err == nil {
		if e.Initialized() {
			e.Stop()
		}
		if e.chainCtx != nil {
			ctx.(*ChainCtx).Id = e.chainCtx.Id
		}
		e.chainCtx = ctx.(*ChainCtx)
		//设置子规则链池
		e.chainCtx.SetRuleChainPool(e.RuleChainPool)

		return nil
	} else {
		return err
	}
}

// ReloadChild 更新根规则链或者其下某个节点
//如果ruleNodeId为空更新根规则链，否则更新指定的子节点
//dsl 根规则链/子节点配置
func (e *RuleEngine) ReloadChild(ruleNodeId string, dsl []byte) error {
	if len(dsl) == 0 {
		return errors.New("dsl can not empty")
	} else if e.chainCtx == nil {
		return errors.New("ReloadNode error.RuleEngine not initialized")
	} else if ruleNodeId == "" {
		//更新根规则链
		return e.ReloadSelf(dsl)
	} else {
		//更新根规则链子节点
		return e.chainCtx.ReloadChild(types.OperatorId{Id: ruleNodeId}, dsl)
	}
}

//DSL 获取根规则链配置
func (e *RuleEngine) DSL() []byte {
	if e.chainCtx != nil {
		return e.chainCtx.DSL()
	} else {
		return nil
	}
}

//NodeDSL 获取规则链节点配置
func (e *RuleEngine) NodeDSL(chainId types.OperatorId, childNodeId types.OperatorId) []byte {
	if e.chainCtx != nil {
		if chainId.Id == "" {
			if node, ok := e.chainCtx.GetOperatorById(childNodeId); ok {
				return node.DSL()
			}
		} else {
			if node, ok := e.chainCtx.GetOperatorById(chainId); ok {
				if childNode, ok := node.GetOperatorById(childNodeId); ok {
					return childNode.DSL()
				}
			}
		}
	}
	return nil
}

func (e *RuleEngine) Initialized() bool {
	return e.chainCtx != nil
}

//RootRuleChainCtx 获取根规则链
func (e *RuleEngine) RootRuleChainCtx() *ChainCtx {
	return e.chainCtx
}

func (e *RuleEngine) Stop() {
	if e.chainCtx != nil {
		e.chainCtx.Destroy()
		e.chainCtx = nil
	}
}

// OnMsg 把消息交给规则引擎处理，异步执行
//根据规则链节点配置和连接关系处理消息
func (e *RuleEngine) OnMsg(msg types.RuleMsg) {
	e.OnMsgWithOptions(msg)
}

// OnMsgWithEndFunc 把消息交给规则引擎处理，异步执行
//endFunc 用于数据经过规则链执行完的回调，用于获取规则链处理结果数据。注意：如果规则链有多个结束点，回调函数则会执行多次
func (e *RuleEngine) OnMsgWithEndFunc(msg types.RuleMsg, endFunc func(msg types.RuleMsg, err error)) {
	e.OnMsgWithOptions(msg, types.WithEndFunc(endFunc))
}

// OnMsgWithOptions 把消息交给规则引擎处理，异步执行
//可以携带context选项和结束回调选项
//context 用于不同组件实例数据共享
//endFunc 用于数据经过规则链执行完的回调，用于获取规则链处理结果数据。注意：如果规则链有多个结束点，回调函数则会执行多次
func (e *RuleEngine) OnMsgWithOptions(msg types.RuleMsg, opts ...types.RuleContextOption) {
	e.onMsgAndWait(msg, false, opts...)
}

// OnMsgAndWait 把消息交给规则引擎处理，同步执行，等规则链所有节点执行完，返回
func (e *RuleEngine) OnMsgAndWait(msg types.RuleMsg, opts ...types.RuleContextOption) {
	e.onMsgAndWait(msg, true, opts...)
}

func (e *RuleEngine) onMsgAndWait(msg types.RuleMsg, wait bool, opts ...types.RuleContextOption) {
	if e.chainCtx == nil { // 沒有定义根则链或者没初始化
		e.Config.Logger.Printf("onMsg error.RuleEngine not initialized")
		return
	}

	rootOpCtx := e.chainCtx.rootOperatorCtx.(*DefaultOperatorContext)
	rootCtxCopy := NewOperatorContext(
		rootOpCtx.GetContext(),
		rootOpCtx.config,
		rootOpCtx.chainCtx,
		rootOpCtx.from,
		rootOpCtx.self,
		rootOpCtx.pool,
		rootOpCtx.onEnd,
		e.RuleChainPool,
	)
	rootCtxCopy.isFirst = rootOpCtx.isFirst
	for _, opt := range opts {
		opt(rootCtxCopy)
	}
	rootCtxCopy.TellNext(msg)


	//同步方式调用，等规则链都执行完，才返回
	if wait {
		customFunc := rootCtxCopy.onAllNodeCompleted
		c := make(chan struct{})
		rootCtxCopy.onAllNodeCompleted = func() {
			close(c)
			if customFunc != nil {
				//触发自定义回调
				customFunc()
			}
		}
		<-c
	}
}

// NewConfig creates a new EngineConfig and applies the options.
func NewConfig(opts ...types.Option) types.EngineConfig {
	c := types.NewConfig(opts...)
	if c.Parser == nil {
		c.Parser = &JsonParser{}
	}
	if c.ComponentsRegistry == nil {
		c.ComponentsRegistry = Registry
	}
	return c
}

// RuleEngineOption is a function type that modifies the RuleEngine.
type RuleEngineOption func(*RuleEngine) error

// WithConfig is an option that sets the EngineConfig of the RuleEngine.
func WithConfig(config types.EngineConfig) RuleEngineOption {
	return func(re *RuleEngine) error {
		re.Config = config
		return nil
	}
}

//WithRuleChainPool 子规则链池
func WithRuleChainPool(ruleChainPool *RuleGo) RuleEngineOption {
	return func(re *RuleEngine) error {
		re.RuleChainPool = ruleChainPool
		return nil
	}
}
