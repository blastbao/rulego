package rulego

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/rulego/rulego/api/types"
)

// OperatorContext 默认规则引擎消息处理上下文
//
// 每个节点在执行时，都会运行在自己的 ctx 中，整个运行图是 flow context 节点连接构成的。
type OperatorContext struct {
	/// global
	context  context.Context
	chainCtx *ChainCtx

	//上一个节点上下文
	from types.OperatorRuntime
	//当前节点上下文
	self types.OperatorRuntime
	//是否是第一个节点
	isFirst bool

	//父ruleContext
	parent *OperatorContext
	//当前节点下未执行完成的子节点数量
	waitingCount int32

	/// callback functions
	//当前消息整条规则链处理结束回调函数
	onEnd func(msg types.RuleMsg, err error)
	//所有子节点处理完成事件，只执行一次
	onCompleted func()
}

//NewOperatorContext 创建一个默认规则引擎消息处理上下文实例
func NewOperatorContext(
	context context.Context,
	chainCtx *ChainCtx,
	from types.OperatorRuntime,
	self types.OperatorRuntime,
	onEnd func(msg types.RuleMsg, err error),
) *OperatorContext {
	return &OperatorContext{
		context:       context,
		chainCtx:      chainCtx,
		from:          from,
		self:          self,
		isFirst:       from == nil,
		onEnd:         onEnd,
	}
}

//NewNextOperatorContext 创建下一个节点的规则引擎消息处理上下文实例RuleContext
func (ctx *OperatorContext) NewNextOperatorContext(nextNode types.OperatorRuntime) *OperatorContext {
	return &OperatorContext{
		chainCtx:      ctx.chainCtx,
		onEnd:         ctx.onEnd,
		from:          ctx.self,
		self:          nextNode,         // 当前节点 OperatorRuntime
		context:       ctx.GetContext(), // 继承 golang Context
		parent:        ctx,              // 新节点的 parent 为本节点
		waitingCount:  0,                // 当前节点的正在运行的 child 节点数
	}
}

func (ctx *OperatorContext) TellSuccess(msg types.RuleMsg) {
	ctx.tell(msg, nil, types.Success)
}
func (ctx *OperatorContext) TellFailure(msg types.RuleMsg, err error) {
	ctx.tell(msg, err, types.Failure)
}
func (ctx *OperatorContext) TellNext(msg types.RuleMsg, relationTypes ...string) {
	ctx.tell(msg, nil, relationTypes...)
}
func (ctx *OperatorContext) TellSelf(msg types.RuleMsg, delayMs int64) {
	time.AfterFunc(time.Millisecond*time.Duration(delayMs), func() {
		_ = ctx.self.OnMsg(ctx, msg)
	})
}
func (ctx *OperatorContext) NewMsg(msgType string, metaData types.Metadata, data string) types.RuleMsg {
	return types.NewMsg(0, msgType, types.JSON, metaData, data)
}
func (ctx *OperatorContext) GetSelfId() string {
	return ctx.self.GetOperatorId().Id
}

func (ctx *OperatorContext) SetEndFunc(onEndFunc func(msg types.RuleMsg, err error)) types.OperatorContext {
	ctx.onEnd = onEndFunc
	return ctx
}

func (ctx *OperatorContext) GetEndFunc() func(msg types.RuleMsg, err error) {
	return ctx.onEnd
}

func (ctx *OperatorContext) SetContext(c context.Context) types.OperatorContext {
	ctx.context = c
	return ctx
}

func (ctx *OperatorContext) GetContext() context.Context {
	return ctx.context
}

func (ctx *OperatorContext) SetAllCompletedFunc(f func()) types.OperatorContext {
	ctx.onCompleted = f
	return ctx
}

func (ctx *OperatorContext) SubmitTack(task func()) {
	if ctx.chainCtx.Engine.Config.Pool != nil {
		if err := ctx.chainCtx.Engine.Config.Pool.Submit(task); err != nil {
			ctx.chainCtx.Engine.Config.Logger.Printf("SubmitTack error:%s", err)
		}
	} else {
		go task()
	}
}

//TellFlow 执行子规则链，ruleChainId 规则链ID
//onEndFunc 子规则链链分支执行完的回调，并返回该链执行结果，如果同时触发多个分支链，则会调用多次
//onCompleted 所以节点执行完之后的回调，无结果返回
//如果找不到规则链，并把消息通过`Failure`关系发送到下一个节点
func (ctx *OperatorContext) TellFlow(msg types.RuleMsg, chainId string, onEndFunc func(msg types.RuleMsg, err error), onAllNodeCompleted func()) {
	if e, ok := ctx.chainCtx.Engine.Engines.Get(chainId); ok {
		e.OnMsgWithOptions(msg, types.WithEndFunc(onEndFunc), types.WithOnAllNodeCompleted(onAllNodeCompleted))
	} else {
		ctx.TellFailure(msg, fmt.Errorf("ruleChain id=%s not found", chainId))
	}
}

//SetOnAllNodeCompleted 设置所有节点执行完回调
func (ctx *OperatorContext) SetOnAllNodeCompleted(onAllNodeCompleted func()) {
	ctx.onCompleted = onAllNodeCompleted
}

//增加当前节点正在执行的子节点数
func (ctx *OperatorContext) childReady() {
	atomic.AddInt32(&ctx.waitingCount, 1)
}

//减少当前节点正在执行的子节点数
//如果返回数量0，表示该分支链条已经都执行完成，递归父节点，直到所有节点都处理完，则触发onAllNodeCompleted事件。
func (ctx *OperatorContext) childDone() {
	if atomic.AddInt32(&ctx.waitingCount, -1) <= 0 {
		//该节点已经执行完成，通知父节点
		if ctx.parent != nil {
			ctx.parent.childDone()
		}
		//完成回调
		if ctx.onCompleted != nil {
			ctx.onCompleted()
		}
	}
}

// getToOperators 获取当前节点指定关系的子节点
func (ctx *OperatorContext) getToOperators(connType string) ([]types.OperatorRuntime, bool) {
	if ctx.chainCtx == nil || ctx.self == nil {
		return nil, false
	}
	return ctx.chainCtx.GetToOperators(ctx.self.GetOperatorId(), connType)
}

func (ctx *OperatorContext) onDebug(flowType string, nodeId string, msg types.RuleMsg, relationType string, err error) {
	if ctx.chainCtx.Engine.Config.OnDebug != nil {
		var chainId = ""
		if ctx.chainCtx != nil {
			chainId = ctx.chainCtx.Id.Id
		}
		ctx.chainCtx.Engine.Config.OnDebug(chainId, flowType, nodeId, msg.Copy(), relationType, err)
	}
}

// 这里 tell 的执行是非阻塞的
func (ctx *OperatorContext) tell(rawMsg types.RuleMsg, err error, connTypes ...string) {
	msg := rawMsg.Copy()
	if ctx.isFirst {
		ctx.SubmitTack(func() {
			if ctx.self != nil {
				ctx.tellNext(msg, ctx.self)
			} else {
				/// 异常情况？isFirst 意味着 from == nil ，这里 self == nil ，那就是空的 chain ？
				ctx.doOnEnd(msg, err)
			}
		})
	} else {

		for _, typ := range connTypes {
			// 记录调试信息
			if ctx.self != nil && ctx.self.IsDebugMode() {
				ctx.SubmitTack(func() {
					ctx.onDebug(types.Out, ctx.GetSelfId(), msg, typ, err)
				})
			}
			// [重要] ctx 主要用户获取上、下游节点，并将消息转发过去。
			if ops, ok := ctx.getToOperators(typ); ok {
				/// 存在 typ 类型后继节点，转发 msg ；
				for _, op := range ops {
					op := op
					ctx.SubmitTack(func() {
						ctx.tellNext(msg.Copy(), op)
					})
				}
			} else {
				/// ??? 每个 typ 都调用一次 OnEnd() ???
				///
				/// 如果当前节点不存在 typ 类型后继节点，则当前节点为某链路的 end 节点，调用 'OnEnd()' 回调。
				/// msg 为当前 end 节点接收的整条链路最后的 msg 。
				ctx.doOnEnd(msg, err)
			}
		}
	}

}

/// [重要]
func (ctx *OperatorContext) tellNext(msg types.RuleMsg, to types.OperatorRuntime) {
	toCtx := ctx.NewNextOperatorContext(to)

	// 增加当前节点正在执行的子节点数
	ctx.childReady()
	defer func() {
		//捕捉异常
		if e := recover(); e != nil {
			if toCtx.self != nil && toCtx.self.IsDebugMode() {
				//记录异常信息
				ctx.onDebug(types.In, toCtx.GetSelfId(), msg, "", fmt.Errorf("%v", e))
			}
			// 发生异常，意味着子节点执行完成(失败)，减少正在执行子节点数
			ctx.childDone()
		}
	}()

	if toCtx.self != nil && toCtx.self.IsDebugMode() {
		//记录调试信息
		ctx.onDebug(types.In, toCtx.GetSelfId(), msg, "", nil)
	}

	// 将消息转发给该子节点
	if err := to.OnMsg(toCtx, msg); err != nil {
		ctx.chainCtx.Engine.Config.Logger.Printf("tellNext error.node type:%s error: %s", toCtx.self.Type(), err)
	}

}

//规则链执行完成回调函数
///
///
func (ctx *OperatorContext) doOnEnd(msg types.RuleMsg, err error) {
	//全局回调
	//通过`Configuration.OnEnd`设置
	if ctx.chainCtx.Engine.Config.OnEnd != nil {
		ctx.SubmitTack(func() {
			ctx.chainCtx.Engine.Config.OnEnd(msg, err)
		})
	}

	//单条消息的context回调
	//通过OnMsgWithEndFunc(msg, endFunc)设置
	if ctx.onEnd != nil {
		ctx.SubmitTack(func() {
			ctx.onEnd(msg, err) // 回调
			ctx.childDone()		// 结束
		})
	} else {
		ctx.childDone()			// 结束
	}
}
