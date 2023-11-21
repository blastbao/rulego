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
)

var GEngines = &Engines{}

//LoadEngines 加载指定文件夹及其子文件夹所有规则链配置（与.json结尾文件），到规则引擎实例池
//规则链ID，使用文件配置的 ruleChain.id
func LoadEngines(dir string, opts ...EngineOption) error {
	return GEngines.Load(dir, opts...)
}

//NewEngine 创建一个新的RuleEngine并将其存储在RuleGo规则链池中
func NewEngine(id string, chain []byte, opts ...EngineOption) (*Engine, error) {
	return GEngines.New(id, chain, opts...)
}

//GetEngine 获取指定ID规则引擎实例
func GetEngine(id string) (*Engine, bool) {
	return GEngines.Get(id)
}

//DelEngine 删除指定ID规则引擎实例
func DelEngine(id string) {
	GEngines.Del(id)
}

//StopEngines 释放所有规则引擎实例
func StopEngines() {
	GEngines.Stop()
}

//OnMsg 调用所有规则引擎实例处理消息
//规则引擎实例池所有规则链都会去尝试处理该消息
func OnMsg(msg types.RuleMsg) {
	GEngines.OnMsg(msg)
}
