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

// Engine 规则引擎
// 每个规则引擎实例只有一个根规则链，如果没设置规则链则无法处理数据
type Engine struct {
	//规则引擎实例标识
	Id string
	//配置
	Config types.Configuration
	//子规则链池
	ChainPool *Engines
	//根规则链
	chainCtx *ChainCtx
}

func newEngine(id string, cfg []byte, opts ...EngineOption) (*Engine, error) {
	if len(cfg) == 0 {
		return nil, errors.New("def can not nil")
	}
	// Create a new Configuration with the Id
	engine := &Engine{
		Id:        id,
		Config:    NewConfig(),
		ChainPool: GEngines,
	}
	if err := engine.Reload(cfg, opts...); err != nil {
		return nil, err
	}
	if engine.chainCtx != nil {
		if id != "" {
			engine.chainCtx.Id = types.OperatorId{Id: id, Type: types.CHAIN}
		} else {
			//使用规则链ID
			engine.Id = engine.chainCtx.Id.Id
		}
	}
	return engine, nil
}

// Reload 重新加载规则链
func (e *Engine) Reload(cfg []byte, opts ...EngineOption) error {
	for _, opt := range opts {// Apply the options to the Configuration.
		_ = opt(e)
	}
	chain, err := ParseChain(cfg)
	if err != nil {
		return err
	}
	chainCtx, err := NewChainCtx(e.Config, &chain)
	if err != nil {
		return err
	}
	if e.chainCtx != nil {
		chainCtx.Id = e.chainCtx.Id
	}
	if e.Initialized() {
		e.Stop()
	}
	e.chainCtx = chainCtx
	e.chainCtx.SetEngines(e.ChainPool) //设置子规则链池
	return nil

}

// ReloadChild 更新根规则链或者其下某个节点
//如果ruleNodeId为空更新根规则链，否则更新指定的子节点
//dsl 根规则链/子节点配置
func (e *Engine) ReloadChild(ruleNodeId string, dsl []byte) error {
	if len(dsl) == 0 {
		return errors.New("dsl can not empty")
	} else if e.chainCtx == nil {
		return errors.New("ReloadNode error.Configuration not initialized")
	} else if ruleNodeId == "" {
		//更新根规则链
		return e.Reload(dsl)
	} else {
		//更新根规则链子节点
		return e.chainCtx.ReloadChild(types.OperatorId{Id: ruleNodeId}, dsl)
	}
}

//DSL 获取根规则链配置
func (e *Engine) DSL() []byte {
	if e.chainCtx != nil {
		return e.chainCtx.DSL()
	} else {
		return nil
	}
}

//NodeDSL 获取规则链节点配置
func (e *Engine) NodeDSL(chainId types.OperatorId, childNodeId types.OperatorId) []byte {
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

func (e *Engine) Initialized() bool {
	return e.chainCtx != nil
}

//RootRuleChainCtx 获取根规则链
func (e *Engine) RootRuleChainCtx() *ChainCtx {
	return e.chainCtx
}

func (e *Engine) Stop() {
	if e.chainCtx != nil {
		e.chainCtx.Destroy()
		e.chainCtx = nil
	}
}

// OnMsg 把消息交给规则引擎处理，异步执行
//根据规则链节点配置和连接关系处理消息
func (e *Engine) OnMsg(msg types.RuleMsg) {
	e.OnMsgWithOptions(msg)
}

// OnMsgWithEndFunc 把消息交给规则引擎处理，异步执行
//endFunc 用于数据经过规则链执行完的回调，用于获取规则链处理结果数据。注意：如果规则链有多个结束点，回调函数则会执行多次
func (e *Engine) OnMsgWithEndFunc(msg types.RuleMsg, endFunc func(msg types.RuleMsg, err error)) {
	e.OnMsgWithOptions(msg, types.WithEndFunc(endFunc))
}

// OnMsgWithOptions 把消息交给规则引擎处理，异步执行
//可以携带context选项和结束回调选项
//context 用于不同组件实例数据共享
//endFunc 用于数据经过规则链执行完的回调，用于获取规则链处理结果数据。注意：如果规则链有多个结束点，回调函数则会执行多次
func (e *Engine) OnMsgWithOptions(msg types.RuleMsg, opts ...types.RuleContextOption) {
	e.onMsgAndWait(msg, false, opts...)
}

// OnMsgAndWait 把消息交给规则引擎处理，同步执行，等规则链所有节点执行完，返回
func (e *Engine) OnMsgAndWait(msg types.RuleMsg, opts ...types.RuleContextOption) {
	e.onMsgAndWait(msg, true, opts...)
}

func (e *Engine) onMsgAndWait(msg types.RuleMsg, wait bool, opts ...types.RuleContextOption) {
	if e.chainCtx == nil { // 沒有定义根则链或者没初始化
		e.Config.Logger.Printf("onMsg error.Configuration not initialized")
		return
	}

	rootOpCtx := e.chainCtx.rootOperatorCtx.(*DefaultOperatorContext)
	rootCtxCopy := NewOperatorContext(
		rootOpCtx.GetContext(),
		rootOpCtx.engine,
		rootOpCtx.chainCtx,
		rootOpCtx.from,
		rootOpCtx.self,
		rootOpCtx.onEnd,
	)
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

// NewConfig creates a new Engine and applies the options.
func NewConfig(opts ...types.Option) types.Configuration {
	c := types.NewConfiguration(opts...)
	if c.Parser == nil {
		c.Parser = &JsonParser{}
	}
	if c.Registry == nil {
		c.Registry = Registry
	}
	return c
}

// EngineOption is a function type that modifies the Engine.
type EngineOption func(*Engine) error

// WithConfig is an option that sets the Engine of the Engine.
func WithConfig(cfg types.Configuration) EngineOption {
	return func(re *Engine) error {
		re.Config = cfg
		return nil
	}
}

//WithRuleChainPool 子规则链池
func WithRuleChainPool(ruleChainPool *Engines) EngineOption {
	return func(re *Engine) error {
		re.ChainPool = ruleChainPool
		return nil
	}
}
