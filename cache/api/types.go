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

package api

import (
	"runtime"
	"sync"
	"time"

	apisecurity "github.com/polarismesh/specification/source/go/api/v1/security"
	apiservice "github.com/polarismesh/specification/source/go/api/v1/service_manage"
	apitraffic "github.com/polarismesh/specification/source/go/api/v1/traffic_manage"

	"github.com/polarismesh/polaris/common/metrics"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/store"
)

const (
	AllMatched = "*"
)

const (
	// NamespaceName cache name
	NamespaceName = "namespace"
	// ServiceName
	ServiceName = "service"
	// InstanceName instance name
	InstanceName = "instance"
	// L5Name l5 name
	L5Name = "l5"
	// RoutingConfigName router config name
	RoutingConfigName = "routingConfig"
	// RateLimitConfigName rate limit config name
	RateLimitConfigName = "rateLimitConfig"
	// CircuitBreakerName circuit breaker config name
	CircuitBreakerName = "circuitBreakerConfig"
	// FaultDetectRuleName fault detect config name
	FaultDetectRuleName = "faultDetectRule"
	// ConfigGroupCacheName config group config name
	ConfigGroupCacheName = "configGroup"
	// ConfigFileCacheName config file config name
	ConfigFileCacheName = "configFile"
	// ClientName client cache name
	ClientName = "client"
	// UsersName user data config name
	UsersName = "users"
	// StrategyRuleName strategy rule config name
	StrategyRuleName = "strategyRule"
	// ServiceContractName service contract config name
	ServiceContractName = "serviceContract"
)

type CacheIndex int

const (
	// CacheNamespace int = iota
	// CacheBusiness
	CacheService CacheIndex = iota
	CacheInstance
	CacheRoutingConfig
	CacheCL5
	CacheRateLimit
	CacheCircuitBreaker
	CacheUser
	CacheAuthStrategy
	CacheNamespace
	CacheClient
	CacheConfigFile
	CacheFaultDetector
	CacheConfigGroup
	CacheServiceContract

	CacheLast
)

// Cache 缓存接口
type Cache interface {
	// Initialize
	Initialize(c map[string]interface{}) error
	// Update .
	Update() error
	// Clear .
	Clear() error
	// Name .
	Name() string
	// Close .
	Close() error
}

// CacheManager
type CacheManager interface {
	// GetCacher
	GetCacher(cacheIndex CacheIndex) Cache
	// RegisterCacher
	RegisterCacher(cacheIndex CacheIndex, item Cache)
}

type (
	// NamespaceCache 命名空间的 Cache 接口
	NamespaceCache interface {
		Cache
		// GetNamespace get target namespace by id
		GetNamespace(id string) *model.Namespace
		// GetNamespacesByName list all namespace by name
		GetNamespacesByName(names []string) []*model.Namespace
		// GetNamespaceList list all namespace
		GetNamespaceList() []*model.Namespace
		// GetVisibleNamespaces list target namespace can visible other namespaces
		GetVisibleNamespaces(namespace string) []*model.Namespace
	}
)

type (
	// ServiceIterProc 迭代回调函数
	ServiceIterProc func(key string, value *model.Service) (bool, error)

	// ServiceArgs 服务查询条件
	ServiceArgs struct {
		// Filter 普通服务字段条件
		Filter map[string]string
		// Metadata 元数据条件
		Metadata map[string]string
		// SvcIds 是否按照服务的ID进行等值查询
		SvcIds map[string]struct{}
		// WildName 是否进行名字的模糊匹配
		WildName bool
		// WildBusiness 是否进行业务的模糊匹配
		WildBusiness bool
		// WildNamespace 是否进行命名空间的模糊匹配
		WildNamespace bool
		// Namespace 条件中的命名空间
		Namespace string
		// Name 条件中的服务名
		Name string
		// EmptyCondition 是否是空条件，即只需要从所有服务或者某个命名空间下面的服务，进行不需要匹配的遍历，返回前面的服务即可
		EmptyCondition bool
	}

	// ServiceCache 服务数据缓存接口
	ServiceCache interface {
		Cache
		// GetNamespaceCntInfo Return to the service statistics according to the namespace,
		// 	the count statistics and health instance statistics
		GetNamespaceCntInfo(namespace string) model.NamespaceServiceCount
		// GetAllNamespaces Return all namespaces
		GetAllNamespaces() []string
		// GetServiceByID According to ID query service information
		GetServiceByID(id string) *model.Service
		// GetServiceByName Inquiry service information according to service name
		GetServiceByName(name string, namespace string) *model.Service
		// IteratorServices Iterative Cache Service Information
		IteratorServices(iterProc ServiceIterProc) error
		// CleanNamespace Clear the cache of NameSpace
		CleanNamespace(namespace string)
		// GetServicesCount Get the number of services in the cache
		GetServicesCount() int
		// GetServiceByCl5Name Get the corresponding SID according to CL5name
		GetServiceByCl5Name(cl5Name string) *model.Service
		// GetServicesByFilter Serving the service filtering in the cache through Filter
		GetServicesByFilter(serviceFilters *ServiceArgs,
			instanceFilters *store.InstanceArgs, offset, limit uint32) (uint32, []*model.EnhancedService, error)
		// ListServices get service list and revision by namespace
		ListServices(ns string) (string, []*model.Service)
		// ListAllServices get all service and revision
		ListAllServices() (string, []*model.Service)
		// ListServiceAlias list service link alias list
		ListServiceAlias(namespace, name string) []*model.Service
		// GetAliasFor get alias reference service info
		GetAliasFor(name string, namespace string) *model.Service
		// GetRevisionWorker .
		GetRevisionWorker() ServiceRevisionWorker
		// GetVisibleServicesInOtherNamespace get same service in other namespace and it's visible
		GetVisibleServicesInOtherNamespace(name string, namespace string) []*model.Service
	}

	// ServiceRevisionWorker
	ServiceRevisionWorker interface {
		// Notify
		Notify(serviceID string, valid bool)
		// GetServiceRevisionCount
		GetServiceRevisionCount() int
		// GetServiceInstanceRevision
		GetServiceInstanceRevision(serviceID string) string
	}

	// ServiceContractCache .
	ServiceContractCache interface {
		Cache
		// Query .
		Query(filter map[string]string, offset, limit uint32) ([]*model.EnrichServiceContract, uint32, error)
	}
)

type (
	// InstanceIterProc instance iter proc func
	InstanceIterProc func(key string, value *model.Instance) (bool, error)

	// InstanceCache 实例相关的缓存接口
	InstanceCache interface {
		// Cache 公共缓存接口
		Cache
		// GetInstance 根据实例ID获取实例数据
		GetInstance(instanceID string) *model.Instance
		// GetInstancesByServiceID 根据服务名获取实例，先查找服务名对应的服务ID，再找实例列表
		GetInstancesByServiceID(serviceID string) []*model.Instance
		// IteratorInstances 迭代
		IteratorInstances(iterProc InstanceIterProc) error
		// IteratorInstancesWithService 根据服务ID进行迭代
		IteratorInstancesWithService(serviceID string, iterProc InstanceIterProc) error
		// GetInstancesCount 获取instance的个数
		GetInstancesCount() int
		// GetInstancesCountByServiceID 根据服务ID获取实例数
		GetInstancesCountByServiceID(serviceID string) model.InstanceCount
		// GetServicePorts 根据服务ID获取端口号
		GetServicePorts(serviceID string) []*model.ServicePort
		// GetInstanceLabels Get the label of all instances under a service
		GetInstanceLabels(serviceID string) *apiservice.InstanceLabels
		// QueryInstances query instance for OSS
		QueryInstances(filter, metaFilter map[string]string, offset, limit uint32) (uint32, []*model.Instance, error)
		// DiscoverServiceInstances 服务发现获取实例
		DiscoverServiceInstances(serviceID string, onlyHealthy bool) []*model.Instance
	}
)

type (
	// FaultDetectCache  fault detect rule cache service
	FaultDetectCache interface {
		Cache
		// GetFaultDetectConfig 根据ServiceID获取探测配置
		GetFaultDetectConfig(svcName string, namespace string) *model.ServiceWithFaultDetectRules
	}
)

type (
	// RoutingArgs Routing rules query parameters
	RoutingArgs struct {
		// Filter extend filter params
		Filter map[string]string
		// ID route rule id
		ID string
		// Name route rule name
		Name string
		// Service service name
		Service string
		// Namespace namesapce
		Namespace string
		// SourceService source service name
		SourceService string
		// SourceNamespace source service namespace
		SourceNamespace string
		// DestinationService destination service name
		DestinationService string
		// DestinationNamespace destination service namespace
		DestinationNamespace string
		// Enable
		Enable *bool
		// Offset
		Offset uint32
		// Limit
		Limit uint32
		// OrderField Sort field
		OrderField string
		// OrderType Sorting rules
		OrderType string
	}

	// RouterRuleIterProc Method definition of routing rules
	RouterRuleIterProc func(key string, value *model.ExtendRouterConfig)

	// RoutingConfigCache Cache interface configured by routing
	RoutingConfigCache interface {
		Cache
		// GetRouterConfig Obtain routing configuration based on serviceid
		GetRouterConfig(id, service, namespace string) (*apitraffic.Routing, error)
		// GetRouterConfig Obtain routing configuration based on serviceid
		GetRouterConfigV2(id, service, namespace string) (*apitraffic.Routing, error)
		// GetRoutingConfigCount Get the total number of routing configuration cache
		GetRoutingConfigCount() int
		// QueryRoutingConfigsV2 Query Route Configuration List
		QueryRoutingConfigsV2(args *RoutingArgs) (uint32, []*model.ExtendRouterConfig, error)
		// ListRouterRule list all router rule
		ListRouterRule(service, namespace string) []*model.ExtendRouterConfig
		// IsConvertFromV1 Whether the current routing rules are converted from the V1 rule
		IsConvertFromV1(id string) (string, bool)
		// IteratorRouterRule iterator router rule
		IteratorRouterRule(iterProc RouterRuleIterProc)
	}
)

type (
	// RateLimitRuleArgs ratelimit rules query parameters
	RateLimitRuleArgs struct {
		// Filter extend filter params
		Filter map[string]string
		// ID route rule id
		ID string
		// Name route rule name
		Name string
		// Service service name
		Service string
		// Namespace namesapce
		Namespace string
		// Disable *bool
		Disable *bool
		// Offset
		Offset uint32
		// Limit
		Limit uint32
		// OrderField Sort field
		OrderField string
		// OrderType Sorting rules
		OrderType string
	}

	// RateLimitIterProc rate limit iter func
	RateLimitIterProc func(rateLimit *model.RateLimit)

	// RateLimitCache rateLimit的cache接口
	RateLimitCache interface {
		Cache
		// GetRateLimit 根据serviceID进行迭代回调
		IteratorRateLimit(rateLimitIterProc RateLimitIterProc)
		// GetRateLimitRules 根据serviceID获取限流数据
		GetRateLimitRules(serviceKey model.ServiceKey) ([]*model.RateLimit, string)
		// QueryRateLimitRules
		QueryRateLimitRules(args RateLimitRuleArgs) (uint32, []*model.RateLimit, error)
		// GetRateLimitsCount 获取限流规则总数
		GetRateLimitsCount() int
	}
)

type (
	// L5Cache L5的cache接口
	L5Cache interface {
		Cache
		// GetRouteByIP 根据IP获取访问关系
		GetRouteByIP(ip uint32) []*model.Route
		// CheckRouteExisted 检查IP对应的SID是否存在访问关系
		CheckRouteExisted(ip uint32, modID uint32, cmdID uint32) bool
		// GetPolicy 获取有状态路由信息policy
		GetPolicy(modID uint32) *model.Policy
		// GetSection 获取有状态路由信息policy
		GetSection(modeID uint32) []*model.Section
		// GetIPConfig 获取IpConfig
		GetIPConfig(ip uint32) *model.IPConfig
	}
)

type (
	// CircuitBreakerCache  circuitBreaker配置的cache接口
	CircuitBreakerCache interface {
		Cache
		// GetCircuitBreakerConfig 根据ServiceID获取熔断配置
		GetCircuitBreakerConfig(svcName string, namespace string) *model.ServiceWithCircuitBreakerRules
	}
)

type (
	BaseConfigArgs struct {
		// Namespace
		Namespace string
		// Group
		Group string
		// Offset
		Offset uint32
		// Limit
		Limit uint32
		// OrderField Sort field
		OrderField string
		// OrderType Sorting rules
		OrderType string
	}

	ConfigFileArgs struct {
		BaseConfigArgs
		FileName string
		Metadata map[string]string
	}

	ConfigReleaseArgs struct {
		BaseConfigArgs
		// FileName
		FileName string
		// ReleaseName
		ReleaseName string
		// OnlyActive
		OnlyActive bool
		// Metadata
		Metadata map[string]string
		// NoPage
		NoPage bool
	}

	// ConfigGroupArgs
	ConfigGroupArgs struct {
		Namespace  string
		Name       string
		Business   string
		Department string
		Metadata   map[string]string
		Offset     uint32
		Limit      uint32
		// OrderField Sort field
		OrderField string
		// OrderType Sorting rules
		OrderType string
	}

	// ConfigGroupCache file cache
	ConfigGroupCache interface {
		Cache
		// GetGroupByName
		GetGroupByName(namespace, name string) *model.ConfigFileGroup
		// GetGroupByID
		GetGroupByID(id uint64) *model.ConfigFileGroup
		// Query
		Query(args *ConfigGroupArgs) (uint32, []*model.ConfigFileGroup, error)
	}

	// ConfigFileCache file cache
	ConfigFileCache interface {
		Cache
		// GetActiveRelease
		GetGroupActiveReleases(namespace, group string) ([]*model.ConfigFileRelease, string)
		// GetActiveRelease
		GetActiveRelease(namespace, group, fileName string) *model.ConfigFileRelease
		// GetRelease
		GetRelease(key model.ConfigFileReleaseKey) *model.ConfigFileRelease
		// QueryReleases
		QueryReleases(args *ConfigReleaseArgs) (uint32, []*model.SimpleConfigFileRelease, error)
	}
)

type (
	// UserCache User information cache
	UserCache interface {
		Cache
		// GetAdmin 获取管理员信息
		GetAdmin() *model.User
		// GetUserByID
		GetUserByID(id string) *model.User
		// GetUserByName
		GetUserByName(name, ownerName string) *model.User
		// GetUserGroup
		GetGroup(id string) *model.UserGroupDetail
		// IsUserInGroup 判断 userid 是否在对应的 group 中
		IsUserInGroup(userId, groupId string) bool
		// IsOwner
		IsOwner(id string) bool
		// GetUserLinkGroupIds
		GetUserLinkGroupIds(id string) []string
	}

	// StrategyCache is a cache for strategy rules.
	StrategyCache interface {
		Cache
		// GetStrategyDetailsByUID
		GetStrategyDetailsByUID(uid string) []*model.StrategyDetail
		// GetStrategyDetailsByGroupID returns all strategy details of a group.
		GetStrategyDetailsByGroupID(groupId string) []*model.StrategyDetail
		// IsResourceLinkStrategy 该资源是否关联了鉴权策略
		IsResourceLinkStrategy(resType apisecurity.ResourceType, resId string) bool
		// IsResourceEditable 判断该资源是否可以操作
		IsResourceEditable(principal model.Principal, resType apisecurity.ResourceType, resId string) bool
		// ForceSync 强制同步鉴权策略到cache (串行)
		ForceSync() error
	}
)

type (

	// ClientIterProc client iter proc func
	ClientIterProc func(key string, value *model.Client) bool

	// ClientCache 客户端的 Cache 接口
	ClientCache interface {
		Cache
		// GetClient get client
		GetClient(id string) *model.Client
		// IteratorClients 迭代
		IteratorClients(iterProc ClientIterProc)
		// GetClientsByFilter Query client information
		GetClientsByFilter(filters map[string]string, offset, limit uint32) (uint32, []*model.Client, error)
	}
)

var (
	// DefaultTimeDiff default time diff
	DefaultTimeDiff = -5 * time.Second
)

// BaseCache 对于 Cache 中的一些 func 做统一实现，避免重复逻辑
type BaseCache struct {
	lock sync.RWMutex
	// firstUpdate Whether the cache is loaded for the first time
	// this field can only make value on exec initialize/clean, and set it to false on exec update
	firstUpdate   bool
	s             store.Store
	lastFetchTime int64
	lastMtimes    map[string]time.Time
	CacheMgr      CacheManager
}

func NewBaseCache(s store.Store, cacheMgr CacheManager) *BaseCache {
	c := &BaseCache{
		s:        s,
		CacheMgr: cacheMgr,
	}

	c.initialize()
	return c
}

func (bc *BaseCache) initialize() {
	bc.lock.Lock()
	defer bc.lock.Unlock()

	bc.lastFetchTime = 1
	bc.firstUpdate = true
	bc.lastMtimes = map[string]time.Time{}
}

var (
	zeroTime = time.Unix(0, 0)
)

func (bc *BaseCache) Store() store.Store {
	return bc.s
}

func (bc *BaseCache) ResetLastMtime(label string) {
	bc.lock.Lock()
	defer bc.lock.Unlock()
	bc.lastMtimes[label] = time.Unix(0, 0)
}

func (bc *BaseCache) ResetLastFetchTime() {
	bc.lock.Lock()
	defer bc.lock.Unlock()
	bc.lastFetchTime = 1
}

func (bc *BaseCache) LastMtime(label string) time.Time {
	bc.lock.RLock()
	defer bc.lock.RUnlock()
	v, ok := bc.lastMtimes[label]
	if ok {
		return v
	}

	return time.Unix(0, 0)
}

func (bc *BaseCache) LastFetchTime() time.Time {
	lastTime := time.Unix(bc.lastFetchTime, 0)
	tmp := lastTime.Add(DefaultTimeDiff)
	if zeroTime.After(tmp) {
		return lastTime
	}
	lastTime = tmp
	return lastTime
}

// OriginLastFetchTime only for test
func (bc *BaseCache) OriginLastFetchTime() time.Time {
	lastTime := time.Unix(bc.lastFetchTime, 0)
	return lastTime
}

func (bc *BaseCache) IsFirstUpdate() bool {
	return bc.firstUpdate
}

// update
func (bc *BaseCache) DoCacheUpdate(name string, executor func() (map[string]time.Time, int64, error)) error {
	if bc.IsFirstUpdate() {
		log.Infof("[Cache][%s] begin run cache update work", name)
	}

	curStoreTime, err := bc.s.GetUnixSecond(0)
	if err != nil {
		curStoreTime = bc.lastFetchTime
		log.Warnf("[Cache][%s] get store timestamp fail, skip update lastMtime, err : %v", name, err)
	}
	defer func() {
		if err := recover(); err != nil {
			var buf [4086]byte
			n := runtime.Stack(buf[:], false)
			log.Errorf("[Cache][%s] run cache update panic: %+v, stack\n%s\n", name, err, string(buf[:n]))
		} else {
			bc.lastFetchTime = curStoreTime
		}
	}()

	start := time.Now()
	lastMtimes, total, err := executor()
	if err != nil {
		return err
	}

	bc.lock.Lock()
	defer bc.lock.Unlock()
	if len(lastMtimes) != 0 {
		if len(bc.lastMtimes) != 0 {
			for label, lastMtime := range lastMtimes {
				preLastMtime := bc.lastMtimes[label]
				log.Infof("[Cache][%s] lastFetchTime %s, lastMtime update from %s to %s",
					label, time.Unix(bc.lastFetchTime, 0), preLastMtime, lastMtime)
			}
		}
		bc.lastMtimes = lastMtimes
	}

	if total >= 0 {
		metrics.RecordCacheUpdateCost(time.Since(start), name, total)
	}
	bc.firstUpdate = false
	return nil
}

func (bc *BaseCache) Clear() {
	bc.lock.Lock()
	defer bc.lock.Unlock()
	bc.lastMtimes = make(map[string]time.Time)
	bc.lastFetchTime = 1
	bc.firstUpdate = true
}

func (bc *BaseCache) Close() error {
	return nil
}
