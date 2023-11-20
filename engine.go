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
	"errors"
	"fmt"
	"github.com/rulego/rulego/api/types"
	"sync/atomic"
	"time"
)

// DefaultOperatorContext 默认规则引擎消息处理上下文
//
//
// 每个节点在执行时，都会运行在自己的 ctx 中，整个运行图是 flow context 节点连接构成的。
//
type DefaultOperatorContext struct {
	//id     string
	//用于不同组件共享信号量和数据的上下文
	context context.Context
	config  types.EngineConfig
	//根规则链上下文
	chainCtx *ChainCtx
	//上一个节点上下文
	from types.OperatorRuntime
	//当前节点上下文
	self types.OperatorRuntime
	//是否是第一个节点
	isFirst bool
	//协程池
	pool types.Pool
	//当前消息整条规则链处理结束回调函数
	onEnd func(msg types.RuleMsg, err error)
	//当前节点下未执行完成的子节点数量
	waitingCount int32
	//父ruleContext
	parentRuleCtx *DefaultOperatorContext
	//所有子节点处理完成事件，只执行一次
	onAllNodeCompleted func()
	//子规则链池
	ruleChainPool *RuleGo
}

//NewOperatorContext 创建一个默认规则引擎消息处理上下文实例
func NewOperatorContext(context context.Context,
					config types.EngineConfig,
					ruleChainCtx *ChainCtx,
					from types.OperatorRuntime,
					self types.OperatorRuntime,
					pool types.Pool,
					onEnd func(msg types.RuleMsg, err error),
					ruleChainPool *RuleGo,
) *DefaultOperatorContext {
	return &DefaultOperatorContext{
		context:       context,
		config:        config,
		chainCtx:      ruleChainCtx,
		from:          from,
		self:          self,
		isFirst:       from == nil,
		pool:          pool,
		onEnd:         onEnd,
		ruleChainPool: ruleChainPool,
	}
}

//NewNextNodeRuleContext 创建下一个节点的规则引擎消息处理上下文实例RuleContext
func (ctx *DefaultOperatorContext) NewNextNodeRuleContext(nextNode types.OperatorRuntime) *DefaultOperatorContext {
	return &DefaultOperatorContext{
		config:        ctx.config,
		chainCtx:      ctx.chainCtx,
		from:          ctx.self,
		self:          nextNode,			// 当前节点 OperatorRuntime
		pool:          ctx.pool,
		onEnd:         ctx.onEnd,
		context:       ctx.GetContext(),	// 继承 golang Context
		parentRuleCtx: ctx,					// 新节点的 parent 为本节点

		waitingCount: 0, // 当前节点的正在运行的 child 节点数
	}
}

func (ctx *DefaultOperatorContext) TellSuccess(msg types.RuleMsg) {
	ctx.tell(msg, nil, types.Success)
}
func (ctx *DefaultOperatorContext) TellFailure(msg types.RuleMsg, err error) {
	ctx.tell(msg, err, types.Failure)
}
func (ctx *DefaultOperatorContext) TellNext(msg types.RuleMsg, relationTypes ...string) {
	ctx.tell(msg, nil, relationTypes...)
}
func (ctx *DefaultOperatorContext) TellSelf(msg types.RuleMsg, delayMs int64) {
	time.AfterFunc(time.Millisecond*time.Duration(delayMs), func() {
		_ = ctx.self.OnMsg(ctx, msg)
	})
}
func (ctx *DefaultOperatorContext) NewMsg(msgType string, metaData types.Metadata, data string) types.RuleMsg {
	return types.NewMsg(0, msgType, types.JSON, metaData, data)
}
func (ctx *DefaultOperatorContext) GetSelfId() string {
	return ctx.self.GetOperatorId().Id
}

func (ctx *DefaultOperatorContext) Config() types.EngineConfig {
	return ctx.config
}

func (ctx *DefaultOperatorContext) SetEndFunc(onEndFunc func(msg types.RuleMsg, err error)) types.OperatorContext {
	ctx.onEnd = onEndFunc
	return ctx
}

func (ctx *DefaultOperatorContext) GetEndFunc() func(msg types.RuleMsg, err error) {
	return ctx.onEnd
}

func (ctx *DefaultOperatorContext) SetContext(c context.Context) types.OperatorContext {
	ctx.context = c
	return ctx
}

func (ctx *DefaultOperatorContext) GetContext() context.Context {
	return ctx.context
}

func (ctx *DefaultOperatorContext) SetAllCompletedFunc(f func()) types.OperatorContext {
	ctx.onAllNodeCompleted = f
	return ctx
}

func (ctx *DefaultOperatorContext) SubmitTack(task func()) {
	if ctx.pool != nil {
		if err := ctx.pool.Submit(task); err != nil {
			ctx.config.Logger.Printf("SubmitTack error:%s", err)
		}
	} else {
		go task()
	}
}

//TellFlow 执行子规则链，ruleChainId 规则链ID
//onEndFunc 子规则链链分支执行完的回调，并返回该链执行结果，如果同时触发多个分支链，则会调用多次
//onAllNodeCompleted 所以节点执行完之后的回调，无结果返回
//如果找不到规则链，并把消息通过`Failure`关系发送到下一个节点
func (ctx *DefaultOperatorContext) TellFlow(msg types.RuleMsg, chainId string, onEndFunc func(msg types.RuleMsg, err error), onAllNodeCompleted func()) {
	if e, ok := ctx.GetRuleChainPool().Get(chainId); ok {
		e.OnMsgWithOptions(msg, types.WithEndFunc(onEndFunc), types.WithOnAllNodeCompleted(onAllNodeCompleted))
	} else {
		ctx.TellFailure(msg, fmt.Errorf("ruleChain id=%s not found", chainId))
	}
}

//SetRuleChainPool 设置子规则链池
func (ctx *DefaultOperatorContext) SetRuleChainPool(ruleChainPool *RuleGo) {
	ctx.ruleChainPool = ruleChainPool
}

//GetRuleChainPool 获取子规则链池
func (ctx *DefaultOperatorContext) GetRuleChainPool() *RuleGo {
	if ctx.ruleChainPool == nil {
		return DefaultRuleGo
	} else {
		return ctx.ruleChainPool
	}
}

//SetOnAllNodeCompleted 设置所有节点执行完回调
func (ctx *DefaultOperatorContext) SetOnAllNodeCompleted(onAllNodeCompleted func()) {
	ctx.onAllNodeCompleted = onAllNodeCompleted
}

//增加当前节点正在执行的子节点数
func (ctx *DefaultOperatorContext) childReady() {
	atomic.AddInt32(&ctx.waitingCount, 1)
}

//减少当前节点正在执行的子节点数
//如果返回数量0，表示该分支链条已经都执行完成，递归父节点，直到所有节点都处理完，则触发onAllNodeCompleted事件。
func (ctx *DefaultOperatorContext) childDone() {
	if atomic.AddInt32(&ctx.waitingCount, -1) <= 0 {
		//该节点已经执行完成，通知父节点
		if ctx.parentRuleCtx != nil {
			ctx.parentRuleCtx.childDone()
		}
		//完成回调
		if ctx.onAllNodeCompleted != nil {
			ctx.onAllNodeCompleted()
		}
	}
}

// getToOperators 获取当前节点指定关系的子节点
func (ctx *DefaultOperatorContext) getToOperators(connType string) ([]types.OperatorRuntime, bool) {
	if ctx.chainCtx == nil || ctx.self == nil {
		return nil, false
	}
	return ctx.chainCtx.GetToOperators(ctx.self.GetOperatorId(), connType)
}

func (ctx *DefaultOperatorContext) onDebug(flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
	if ctx.config.OnDebug != nil {
		var chainId = ""
		if ctx.chainCtx != nil {
			chainId = ctx.chainCtx.Id.Id
		}
		ctx.config.OnDebug(chainId, flowType, nodeId, msg.Copy(), relationType, err)
	}
}

func (ctx *DefaultOperatorContext) tell(msg types.RuleMsg, err error, connTypes ...string) {
	msgCopy := msg.Copy()
	if ctx.isFirst {
		ctx.SubmitTack(func() {
			if ctx.self != nil {
				ctx.tellNext(msgCopy, ctx.self)
			} else {
				ctx.doOnEnd(msgCopy, err)
			}
		})
	} else {
		for _, connType := range connTypes {
			if ctx.self != nil && ctx.self.IsDebugMode() {
				//记录调试信息
				ctx.SubmitTack(func() {
					ctx.onDebug(types.Out, ctx.GetSelfId(), msgCopy, connType, err)
				})
			}

			// [重要] ctx 主要用户获取上、下游节点，并将消息转发过去。
			if operators, ok := ctx.getToOperators(connType); ok {
				/// 存在 connType 类型后继节点，转发 msg ；
				for _, operator := range operators {
					operator := operator
					ctx.SubmitTack(func() {
						ctx.tellNext(msg.Copy(), operator)
					})
				}
			} else {
				/// 如果当前节点不存在 connType 类型后继节点，则当前节点为某链路的 end 节点，调用 'OnEnd()' 回调。
				ctx.doOnEnd(msgCopy, err)
			}
		}
	}

}

/// [重要]
func (ctx *DefaultOperatorContext) tellNext(msg types.RuleMsg, nextNode types.OperatorRuntime) {
	nextCtx := ctx.NewNextNodeRuleContext(nextNode)
	// 增加当前节点正在执行的子节点数
	ctx.childReady()
	defer func() {
		//捕捉异常
		if e := recover(); e != nil {
			if nextCtx.self != nil && nextCtx.self.IsDebugMode() {
				//记录异常信息
				ctx.onDebug(types.In, nextCtx.GetSelfId(), msg, "", fmt.Errorf("%v", e))
			}
			ctx.childDone()
		}
	}()

	if nextCtx.self != nil && nextCtx.self.IsDebugMode() {
		//记录调试信息
		ctx.onDebug(types.In, nextCtx.GetSelfId(), msg, "", nil)
	}

	// 将消息转发给该子节点
	if err := nextNode.OnMsg(nextCtx, msg); err != nil {
		ctx.config.Logger.Printf("tellNext error.node type:%s error: %s", nextCtx.self.Type(), err)
	}
}

//规则链执行完成回调函数
func (ctx *DefaultOperatorContext) doOnEnd(msg types.RuleMsg, err error) {
	//全局回调
	//通过`EngineConfig.OnEnd`设置
	if ctx.config.OnEnd != nil {
		ctx.SubmitTack(func() {
			ctx.config.OnEnd(msg, err)
		})
	}

	//单条消息的context回调
	//通过OnMsgWithEndFunc(msg, endFunc)设置
	if ctx.onEnd != nil {
		ctx.SubmitTack(func() {
			ctx.onEnd(msg, err)
			ctx.childDone()
		})
	} else {
		ctx.childDone()
	}

}

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

// RuleEngineOption is a function type that modifies the RuleEngine.
type RuleEngineOption func(*RuleEngine) error

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
	if ctx, err := e.Config.Parser.DecodeRuleChain(e.Config, def); err == nil {
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
	if e.chainCtx == nil {
		//沒有定义根则链或者没初始化
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
