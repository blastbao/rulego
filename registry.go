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
	"fmt"
	"github.com/rulego/rulego/api/types"
	"github.com/rulego/rulego/components/action"
	"github.com/rulego/rulego/components/external"
	"github.com/rulego/rulego/components/filter"
	"github.com/rulego/rulego/components/flow"
	"github.com/rulego/rulego/components/transform"
	"github.com/rulego/rulego/utils/reflect"
	"plugin"
	"sync"
)

//PluginsSymbol 插件检查点 Symbol
const PluginsSymbol = "Plugins"

//Registry 规则引擎组件默认注册器
var Registry = new(OperatorRegistry)

//注册默认组件
func init() {
	var components []types.Operator
	components = append(components, action.Registry.Components()...)
	components = append(components, filter.Registry.Components()...)
	components = append(components, transform.Registry.Components()...)
	components = append(components, external.Registry.Components()...)
	components = append(components, flow.Registry.Components()...)

	//把组件注册到默认组件库
	for _, node := range components {
		_ = Registry.Register(node)
	}
}

//OperatorRegistry 组件注册器
type OperatorRegistry struct {
	//规则引擎节点组件列表
	components map[string]types.Operator
	//插件列表
	plugins map[string][]types.Operator
	sync.RWMutex
}

//Register 注册规则引擎节点组件
func (r *OperatorRegistry) Register(node types.Operator) error {
	r.Lock()
	defer r.Unlock()
	if r.components == nil {
		r.components = make(map[string]types.Operator)
	}
	if _, ok := r.components[node.Type()]; ok {
		return errors.New("the component already exists. nodeType=" + node.Type())
	}
	r.components[node.Type()] = node

	return nil
}

//RegisterPlugin 注册规则引擎节点组件
func (r *OperatorRegistry) RegisterPlugin(name string, file string) error {
	builder := &PluginRegistry{name: name, file: file}
	if err := builder.Init(); err != nil {
		return err
	}
	components := builder.Components()
	for _, node := range components {
		if _, ok := r.components[node.Type()]; ok {
			return errors.New("the component already exists. nodeType=" + node.Type())
		}
	}
	for _, node := range components {
		if err := r.Register(node); err != nil {
			return err
		}
	}

	r.Lock()
	defer r.Unlock()
	if r.plugins == nil {
		r.plugins = make(map[string][]types.Operator)
	}
	r.plugins[name] = components
	return nil
}

func (r *OperatorRegistry) Unregister(componentType string) error {
	r.RLock()
	defer r.RUnlock()
	var removed = false
	// Check if the plugin exists
	if nodes, ok := r.plugins[componentType]; ok {
		for _, node := range nodes {
			// Delete the plugin from the map
			delete(r.components, node.Type())
		}
		delete(r.plugins, componentType)
		removed = true
	}

	// Check if the plugin exists
	if _, ok := r.components[componentType]; ok {
		// Delete the plugin from the map
		delete(r.components, componentType)
		removed = true
	}

	if !removed {
		return fmt.Errorf("component not found.componentType=%s", componentType)
	} else {
		return nil
	}
}

//NewNode 获取规则引擎节点组件
//
//
//
func (r *OperatorRegistry) NewOperator(nodeType string) (types.Operator, error) {
	r.RLock()
	defer r.RUnlock()

	if node, ok := r.components[nodeType]; !ok {
		return nil, fmt.Errorf("component not found.componentType=%s", nodeType)
	} else {
		return node.New(), nil
	}
}

func (r *OperatorRegistry) GetComponents() map[string]types.Operator {
	r.RLock()
	defer r.RUnlock()
	var components = map[string]types.Operator{}
	for k, v := range r.components {
		components[k] = v
	}
	return components
}

func (r *OperatorRegistry) GetComponentForms() types.ComponentFormList {
	r.RLock()
	defer r.RUnlock()

	var components = make(types.ComponentFormList)
	for _, component := range r.components {
		components[component.Type()] = reflect.GetComponentForm(component)
	}
	return components
}

//PluginRegistry go plugin组件初始化器
type PluginRegistry struct {
	name     string
	file     string
	registry types.PluginRegistry
}

func (p *PluginRegistry) Init() error {
	pluginRegistry, err := loadPlugin(p.file)
	if err != nil {
		return err
	} else {
		p.registry = pluginRegistry
		return nil
	}
}

func (p *PluginRegistry) Components() []types.Operator {
	if p.registry != nil {
		return p.registry.Components()
	}
	return nil
}

// loadPlugin loads a plugin from a file and registers it with a given name
func loadPlugin(file string) (types.PluginRegistry, error) {
	// Use the plugin package to open the file and look up the exported symbol "Plugin"
	p, err := plugin.Open(file)
	if err != nil {
		return nil, err
	}
	sym, err := p.Lookup(PluginsSymbol)
	if err != nil {
		return nil, err
	}
	// Use type assertion to check if the symbol is a Plugin interface implementation
	plugin, ok := sym.(types.PluginRegistry)
	if !ok {
		return nil, errors.New("invalid plugin")
	}
	// Register the plugin with the name
	//pm.plugins[name] = plugin
	return plugin, nil
}
