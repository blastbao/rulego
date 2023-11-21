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
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/test/assert"
	"strings"
	"testing"
)

var rootRuleChain = `
	{
	  "ruleChain": {
		"name": "测试规则链",
		"root": true,
		"debugMode": false
	  },
	  "metadata": {
		"ops": [
		  {
			"Id":"s1",
			"type": "jsFilter",
			"name": "过滤",
			"debugMode": true,
			"configuration": {
			  "jsScript": "return msg!='aa';"
			}
		  },
          {
			"Id":"s2",
			"type": "jsTransform",
			"name": "转换",
			"debugMode": true,
			"configuration": {
			  "jsScript": "msgType='TEST_MSG_TYPE2';var msg2={};\n  msg2['aa']=66\n return {'msg':msg,'metadata':metadata,'msgType':msgType};"
			}
		  }
		],
		"cons": [
		  {
			"fromId": "s1",
			"toId": "s2",
			"type": "True"
		  }
		],
		"ruleChainConnections": [
 			{
			"fromId": "s1",
			"toId": "subChain01",
			"type": "True"
		  }
		]
	  }
	}
`
var subRuleChain = `
	{
	  "ruleChain": {
		"name": "测试子规则链",
		"debugMode": false
	  },
	  "metadata": {
		"ops": [
		  {
			"Id":"s1",
			"type": "jsFilter",
			"name": "过滤",
			"debugMode": true,
			"configuration": {
			  "jsScript": "return msg!='aa';"
			}
		  },
			{"Id":"s2",
			"type": "jsTransform",
			"name": "转换",
			"debugMode": true,
			"configuration": {
			  "jsScript": "msgType='TEST_MSG_TYPE2';var msg2={};\n  msg2['aa']=66\n return {'msg':msg,'metadata':metadata,'msgType':msgType};"
			}
		  }
		],
		"cons": [
		  {
			"fromId": "s1",
			"toId": "s2",
			"type": "True"
		  }
 
		]
	  }
	}
`
var s1NodeFile = `
  {
			"Id":"s1",
			"type": "jsFilter",
			"name": "过滤-更改",
			"debugMode": true,
			"configuration": {
			  "jsScript": "return msg!='bb';"
			}
		  }
`

//TestEngine 测试规则引擎
func TestEngine(t *testing.T) {
	config := NewConfig()
	//初始化子规则链
	subRuleEngine, err := NewEngine("subChain01", []byte(subRuleChain), WithConfig(config))
	//初始化根规则链
	ruleEngine, err := NewEngine("rule01", []byte(rootRuleChain), WithConfig(config))
	if err != nil {
		t.Errorf("%v", err)
	}
	assert.True(t, ruleEngine.Initialized())

	//获取节点
	s1NodeId := types.OperatorId{Id: "s1"}
	s1Node, ok := ruleEngine.chainCtx.ops[s1NodeId]
	assert.True(t, ok)
	s1RuleNodeCtx, ok := s1Node.(*OperatorRuntime)
	assert.True(t, ok)
	assert.Equal(t, "过滤", s1RuleNodeCtx.Node.Name)
	assert.Equal(t, "return msg!='aa';", s1RuleNodeCtx.Node.Config["jsScript"])

	//获取子规则链
	subChain01Id := types.OperatorId{Id: "subChain01", Type: types.CHAIN}
	subChain01Node, ok := ruleEngine.chainCtx.GetOperatorById(subChain01Id)
	assert.True(t, ok)
	subChain01NodeCtx, ok := subChain01Node.(*ChainCtx)
	assert.True(t, ok)
	assert.Equal(t, "测试子规则链", subChain01NodeCtx.Chain.Meta.Name)
	assert.Equal(t, subChain01NodeCtx, subRuleEngine.chainCtx)

	//修改根规则链节点
	_ = ruleEngine.ReloadChild(s1NodeId.Id, []byte(s1NodeFile))
	s1Node, ok = ruleEngine.chainCtx.ops[s1NodeId]
	assert.True(t, ok)
	s1RuleNodeCtx, ok = s1Node.(*OperatorRuntime)
	assert.True(t, ok)
	assert.Equal(t, "过滤-更改", s1RuleNodeCtx.Node.Name)
	assert.Equal(t, "return msg!='bb';", s1RuleNodeCtx.Node.Config["jsScript"])

	//修改子规则链
	_ = subRuleEngine.Reload([]byte(strings.Replace(subRuleChain, "测试子规则链", "测试子规则链-更改", -1)))

	subChain01Node, ok = ruleEngine.chainCtx.GetOperatorById(types.OperatorId{Id: "subChain01", Type: types.CHAIN})
	assert.True(t, ok)
	subChain01NodeCtx, ok = subChain01Node.(*ChainCtx)
	assert.True(t, ok)
	assert.Equal(t, "测试子规则链-更改", subChain01NodeCtx.Chain.Meta.Name)

	//获取规则引擎实例
	ruleEngineNew, ok := GetEngine("rule01")
	assert.True(t, ok)
	assert.Equal(t, ruleEngine, ruleEngineNew)
	//删除对应规则引擎实例
	DelEngine("rule01")
	_, ok = GetEngine("rule01")
	assert.False(t, ok)
	assert.False(t, ruleEngine.Initialized())
}
