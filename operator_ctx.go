package rulego

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rulego/rulego/api/types"
)

// DefaultOperatorContext 默认规则引擎消息处理上下文
//
// 每个节点在执行时，都会运行在自己的 ctx 中，整个运行图是 flow context 节点连接构成的。
type DefaultOperatorContext struct {
	//上一个节点上下文
	from types.OperatorRuntime
	//当前节点上下文
	self types.OperatorRuntime
	//是否是第一个节点
	isFirst bool

	/// global

	//id     string
	//用于不同组件共享信号量和数据的上下文
	context context.Context
	config  types.EngineConfig
	//根规则链上下文
	chainCtx *ChainCtx
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
	chainCtx *ChainCtx,
	from types.OperatorRuntime,
	self types.OperatorRuntime,
	pool types.Pool,
	onEnd func(msg types.RuleMsg, err error),
	ruleChainPool *RuleGo,
) *DefaultOperatorContext {
	return &DefaultOperatorContext{
		context:       context,
		config:        config,
		chainCtx:      chainCtx,
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

			// 记录调试信息
			if ctx.self != nil && ctx.self.IsDebugMode() {
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
