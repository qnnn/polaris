/**
 * Tencent is pleased to support the open source community by making Polaris available.
 *
 * Copyright (C) 2019 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package model

import (
	"fmt"
	"time"

	apiservice "github.com/polarismesh/specification/source/go/api/v1/service_manage"
)

type ServiceContract struct {
	ID string
	// 所属命名空间
	Namespace string
	// 所属服务名称
	Service string
	// 契约名称
	Name string
	// 协议，http/grpc/dubbo/thrift
	Protocol string
	// 契约版本
	Version string
	// 信息摘要
	Revision string
	// 额外描述
	Content string
	// 创建时间
	CreateTime time.Time
	// 更新时间
	ModifyTime time.Time
	// 是否有效
	Valid bool
}

type EnrichServiceContract struct {
	*ServiceContract
	// 接口描述信息
	Interfaces []*InterfaceDescriptor
}

func (s *ServiceContract) GetKey() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", s.Namespace, s.Service, s.Name, s.Protocol, s.Version)
}

type InterfaceDescriptor struct {
	// ID
	ID string
	// ContractID
	ContractID string
	// 方法名称，对应 http method/ dubbo interface func/grpc service func
	Method string
	// 接口名称，http path/dubbo interface/grpc service
	Path string
	// 接口描述信息
	Content string
	// 接口信息摘要
	Revision string
	// 创建来源
	Source apiservice.InterfaceDescriptor_Source
	// 创建时间
	CreateTime time.Time
	// 更新时间
	ModifyTime time.Time
	// Valid
	Valid bool
}
