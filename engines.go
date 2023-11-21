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
	"github.com/rulego/rulego/utils/fs"
	"strings"
	"sync"
)


//Engines 规则引擎实例池
type Engines struct {
	engines sync.Map
}

//Load 加载指定文件夹及其子文件夹所有规则链配置（与.json结尾文件），到规则引擎实例池
//规则链ID，使用规则链文件配置的ruleChain.id
func (egs *Engines) Load(dir string, opts ...EngineOption) error {
	if !strings.HasSuffix(dir, "*.json") && !strings.HasSuffix(dir, "*.JSON") {
		if strings.HasSuffix(dir, "/") || strings.HasSuffix(dir, "\\") {
			dir = dir + "*.json"
		} else if dir == "" {
			dir = "./*.json"
		} else {
			dir = dir + "/*.json"
		}
	}
	paths, err := fs.GetFilePaths(dir)
	if err != nil {
		return err
	}
	for _, path := range paths {
		content := fs.LoadFile(path)
		if content != nil {
			if _, err = egs.New("", content, opts...); err != nil {
				return err
			}
		}
	}
	return nil
}

//New 创建一个新的RuleEngine并将其存储在RuleGo规则链池中
//如果指定id="",则使用规则链文件的ruleChain.id
func (egs *Engines) New(id string, chain []byte, opts ...EngineOption) (*Engine, error) {
	if eg, ok := egs.engines.Load(id); ok {
		return eg.(*Engine), nil
	}
	eg, err := newEngine(id, chain, opts...)
	if err != nil {
		return nil, err
	}
	if eg.Id != "" {
		// Store the new Configuration in the engines map with the Id as the key.
		egs.engines.Store(eg.Id, eg)
	}
	return eg, nil
}

//Get 获取指定ID规则引擎实例
func (egs *Engines) Get(id string) (*Engine, bool) {
	v, ok := egs.engines.Load(id)
	if ok {
		return v.(*Engine), ok
	} else {
		return nil, false
	}
}

//Del 删除指定ID规则引擎实例
func (egs *Engines) Del(id string) {
	v, ok := egs.engines.Load(id)
	if ok {
		v.(*Engine).Stop()
		egs.engines.Delete(id)
	}
}

//Stop 释放所有规则引擎实例
func (egs *Engines) Stop() {
	egs.engines.Range(func(key, value interface{}) bool {
		if item, ok := value.(*Engine); ok {
			item.Stop()
		}
		egs.engines.Delete(key)
		return true
	})
}

//OnMsg 调用所有规则引擎实例处理消息
//规则引擎实例池所有规则链都会去尝试处理该消息
func (egs *Engines) OnMsg(msg types.RuleMsg) {
	egs.engines.Range(func(key, value interface{}) bool {
		if item, ok := value.(*Engine); ok {
			item.OnMsg(msg)
		}
		return true
	})
}
