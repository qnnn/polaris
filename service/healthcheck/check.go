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

package healthcheck

import (
	"context"
	"strconv"
	"sync"
	"time"

	apimodel "github.com/polarismesh/specification/source/go/api/v1/model"
	apiservice "github.com/polarismesh/specification/source/go/api/v1/service_manage"
	"go.uber.org/zap"

	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/common/srand"
	commonstore "github.com/polarismesh/polaris/common/store"
	"github.com/polarismesh/polaris/common/timewheel"
	"github.com/polarismesh/polaris/common/utils"
	"github.com/polarismesh/polaris/plugin"
	"github.com/polarismesh/polaris/store"
)

const (
	expireTtlCount = 3
)

// CheckScheduler schedule and run check actions
type CheckScheduler struct {
	rwMutex            *sync.RWMutex
	scheduledInstances map[string]*itemValue
	scheduledClients   map[string]*clientItemValue

	timeWheel              *timewheel.TimeWheel
	minCheckIntervalSec    int64
	maxCheckIntervalSec    int64
	clientCheckIntervalSec int64
	clientCheckTtlSec      int64

	adoptInstancesChan chan AdoptEvent
	ctx                context.Context
}

// AdoptEvent is the event for adopt
type AdoptEvent struct {
	InstanceId string
	Add        bool
	Checker    plugin.HealthChecker
}

type clientItemValue struct {
	itemValue
	lastCheckTimeSec int64
}

type itemValue struct {
	mutex             *sync.Mutex
	id                string
	host              string
	port              uint32
	scheduled         uint32
	ttlDurationSec    uint32
	expireDurationSec uint32
	checker           plugin.HealthChecker
}

type InstanceEventHealthCheckHandler struct {
	ctx                  context.Context
	instanceEventChannel chan *model.InstanceEvent
}

// newLeaderChangeEventHandler
func newInstanceEventHealthCheckHandler(ctx context.Context,
	eventChannel chan *model.InstanceEvent) *InstanceEventHealthCheckHandler {
	return &InstanceEventHealthCheckHandler{
		ctx:                  ctx,
		instanceEventChannel: eventChannel,
	}
}

func (handler *InstanceEventHealthCheckHandler) PreProcess(ctx context.Context, value any) any {
	return value
}

// OnEvent event trigger
func (handler *InstanceEventHealthCheckHandler) OnEvent(ctx context.Context, i interface{}) error {
	e := i.(model.InstanceEvent)
	select {
	case handler.instanceEventChannel <- &e:
		log.Debugf("[Health Check]get instance event, id is %s, type is %s", e.Id, e.EType)
	default:
		log.Errorf("[Health Check]instance event chan full, drop event, id is %s, type is %s", e.Id, e.EType)
	}
	return nil
}

func newCheckScheduler(ctx context.Context, slotNum int, minCheckInterval time.Duration,
	maxCheckInterval time.Duration, clientCheckInterval time.Duration, clientCheckTtl time.Duration) *CheckScheduler {
	scheduler := &CheckScheduler{
		rwMutex:                &sync.RWMutex{},
		scheduledInstances:     make(map[string]*itemValue),
		scheduledClients:       make(map[string]*clientItemValue),
		timeWheel:              timewheel.New(time.Second, slotNum, "health-interval-check"),
		minCheckIntervalSec:    int64(minCheckInterval.Seconds()),
		maxCheckIntervalSec:    int64(maxCheckInterval.Seconds()),
		clientCheckIntervalSec: int64(clientCheckInterval.Seconds()),
		clientCheckTtlSec:      int64(clientCheckTtl.Seconds()),
		adoptInstancesChan:     make(chan AdoptEvent, 1024),
		ctx:                    ctx,
	}
	return scheduler
}

func (c *CheckScheduler) run(ctx context.Context) {
	go c.doCheckInstances(ctx)
	go c.doCheckClient(ctx)
	go c.doAdopt(ctx)
}

func (c *CheckScheduler) doCheckInstances(ctx context.Context) {
	c.timeWheel.Start()
	log.Infof("[Health Check][Check]timeWheel has been started")

	<-ctx.Done()
	c.timeWheel.Stop()
	log.Infof("[Health Check][Check]timeWheel has been stopped")
}

const (
	batchAdoptInterval = 30 * time.Millisecond
	batchAdoptCount    = 30
)

func (c *CheckScheduler) doAdopt(ctx context.Context) {
	instancesToAdd := make(map[string]bool)
	instancesToRemove := make(map[string]bool)
	var checker plugin.HealthChecker
	ticker := time.NewTicker(batchAdoptInterval)
	defer func() {
		ticker.Stop()
	}()
	for {
		select {
		case event := <-c.adoptInstancesChan:
			instanceId := event.InstanceId
			if event.Add {
				instancesToAdd[instanceId] = true
				delete(instancesToRemove, instanceId)
			} else {
				instancesToRemove[instanceId] = true
				delete(instancesToAdd, instanceId)
			}
			checker = event.Checker
			if len(instancesToAdd) == batchAdoptCount {
				instancesToAdd = c.processAdoptEvents(instancesToAdd, true, checker)
			}
			if len(instancesToRemove) == batchAdoptCount {
				instancesToRemove = c.processAdoptEvents(instancesToRemove, false, checker)
			}
		case <-ticker.C:
			if len(instancesToAdd) > 0 {
				instancesToAdd = c.processAdoptEvents(instancesToAdd, true, checker)
			}
			if len(instancesToRemove) > 0 {
				instancesToRemove = c.processAdoptEvents(instancesToRemove, false, checker)
			}
		case <-ctx.Done():
			log.Infof("[Health Check][Check]adopting routine has been stopped")
			return
		}
	}
}

func (c *CheckScheduler) processAdoptEvents(
	instances map[string]bool, add bool, checker plugin.HealthChecker) map[string]bool {
	instanceIds := make([]string, 0, len(instances))
	for id := range instances {
		instanceIds = append(instanceIds, id)
	}
	log.Debug("[Health Check][Check] adopt event", zap.Any("instances", instanceIds),
		zap.String("server", server.localHost), zap.Bool("add", add))
	return instances
}

func (c *CheckScheduler) addAdopting(instanceId string, checker plugin.HealthChecker) {
	select {
	case c.adoptInstancesChan <- AdoptEvent{
		InstanceId: instanceId,
		Add:        true,
		Checker:    checker}:
	case <-c.ctx.Done():
		return
	}
}

func (c *CheckScheduler) removeAdopting(instanceId string, checker plugin.HealthChecker) {
	select {
	case c.adoptInstancesChan <- AdoptEvent{
		InstanceId: instanceId,
		Add:        false,
		Checker:    checker}:
	case <-c.ctx.Done():
		return
	}
}

func (c *CheckScheduler) upsertInstanceChecker(instanceWithChecker *InstanceWithChecker) (bool, *itemValue) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	instance := instanceWithChecker.instance
	ttl := instance.HealthCheck().GetHeartbeat().GetTtl().GetValue()
	var (
		instValue *itemValue
		exist     bool
	)
	instValue, exist = c.scheduledInstances[instance.ID()]
	if exist {
		if ttl == instValue.ttlDurationSec {
			return true, instValue
		}
		// force update check info
		instValue.mutex.Lock()
		oldTtl := instValue.ttlDurationSec
		instValue.checker = instanceWithChecker.checker
		instValue.expireDurationSec = getExpireDurationSec(instance.Proto)
		instValue.ttlDurationSec = ttl
		instValue.mutex.Unlock()
		if log.DebugEnabled() {
			log.Debug("[Health Check][Check] upsert instance checker", zap.String("id", instValue.id),
				zap.Uint32("old-ttl", oldTtl), zap.Uint32("ttl", instValue.ttlDurationSec))
		}
	} else {
		instValue = &itemValue{
			mutex:             &sync.Mutex{},
			host:              instance.Host(),
			port:              instance.Port(),
			id:                instance.ID(),
			expireDurationSec: getExpireDurationSec(instance.Proto),
			checker:           instanceWithChecker.checker,
			ttlDurationSec:    ttl,
		}
	}
	c.scheduledInstances[instance.ID()] = instValue
	return exist, instValue
}

func (c *CheckScheduler) putClientIfAbsent(clientWithChecker *ClientWithChecker) (bool, *clientItemValue) {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	client := clientWithChecker.client
	var instValue *clientItemValue
	var ok bool
	clientId := client.Proto().GetId().GetValue()
	if instValue, ok = c.scheduledClients[clientId]; ok {
		return true, instValue
	}
	instValue = &clientItemValue{
		itemValue: itemValue{
			mutex:             &sync.Mutex{},
			host:              client.Proto().GetHost().GetValue(),
			port:              0,
			id:                clientId,
			expireDurationSec: uint32(expireTtlCount * c.clientCheckTtlSec),
			checker:           clientWithChecker.checker,
			ttlDurationSec:    uint32(c.clientCheckTtlSec),
		},
		lastCheckTimeSec: 0,
	}
	c.scheduledClients[clientId] = instValue
	return false, instValue
}

func (c *CheckScheduler) getInstanceValue(instanceId string) (*itemValue, bool) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	value, ok := c.scheduledInstances[instanceId]
	return value, ok
}

func (c *CheckScheduler) getClientValue(clientId string) (*clientItemValue, bool) {
	c.rwMutex.RLock()
	defer c.rwMutex.RUnlock()
	value, ok := c.scheduledClients[clientId]
	return value, ok
}

// UpsertInstance insert or update instance to check
func (c *CheckScheduler) UpsertInstance(instanceWithChecker *InstanceWithChecker) {
	firstadd, instValue := c.upsertInstanceChecker(instanceWithChecker)
	if firstadd {
		return
	}
	c.addAdopting(instValue.id, instValue.checker)
	instance := instanceWithChecker.instance
	log.Infof("[Health Check][Check]add check instance is %s, host is %s:%d",
		instance.ID(), instance.Host(), instance.Port())
	c.addUnHealthyCallback(instValue)
}

// AddClient add client to check
func (c *CheckScheduler) AddClient(clientWithChecker *ClientWithChecker) {
	exists, instValue := c.putClientIfAbsent(clientWithChecker)
	if exists {
		return
	}
	c.addAdopting(instValue.id, instValue.checker)
	client := clientWithChecker.client
	log.Infof("[Health Check][Check]add check client is %s, host is %s:%d",
		client.Proto().GetId().GetValue(), client.Proto().GetHost(), 0)
}

func getExpireDurationSec(instance *apiservice.Instance) uint32 {
	ttlValue := instance.GetHealthCheck().GetHeartbeat().GetTtl().GetValue()
	return expireTtlCount * ttlValue
}

func getRandDelayMilli() uint32 {
	delayMilli := srand.Intn(1000)
	return uint32(delayMilli)
}

func (c *CheckScheduler) addHealthyCallback(instance *itemValue, lastHeartbeatTimeSec int64) {
	delaySec := instance.expireDurationSec
	var nextDelaySec int64
	if lastHeartbeatTimeSec > 0 {
		curTimeSec := currentTimeSec()
		timePassed := curTimeSec - lastHeartbeatTimeSec
		if timePassed > 0 {
			nextDelaySec = int64(delaySec) - timePassed
		}
	}
	if nextDelaySec > 0 && nextDelaySec < c.minCheckIntervalSec {
		nextDelaySec = c.minCheckIntervalSec
	}
	if nextDelaySec > 0 {
		delaySec = uint32(nextDelaySec)
	}
	host := instance.host
	port := instance.port
	instanceId := instance.id
	delayMilli := delaySec*1000 + getRandDelayMilli()
	log.Debugf("[Health Check][Check]add healthy instance callback, addr is %s:%d, id is %s, delay is %d(ms)",
		host, port, instanceId, delayMilli)
	c.timeWheel.AddTask(delayMilli, instanceId, c.checkCallbackInstance)
}

func (c *CheckScheduler) addUnHealthyCallback(instance *itemValue) {
	delaySec := instance.expireDurationSec
	if c.maxCheckIntervalSec > 0 && int64(delaySec) > c.maxCheckIntervalSec {
		delaySec = uint32(c.maxCheckIntervalSec)
	}
	host := instance.host
	port := instance.port
	instanceId := instance.id
	delayMilli := delaySec*1000 + getRandDelayMilli()
	log.Debugf("[Health Check][Check]add unhealthy instance callback, addr is %s:%d, id is %s, delay is %d(ms)",
		host, port, instanceId, delayMilli)
	c.timeWheel.AddTask(delayMilli, instanceId, c.checkCallbackInstance)
}

func (c *CheckScheduler) checkCallbackClient(clientId string) *clientItemValue {
	clientValue, ok := c.getClientValue(clientId)
	if !ok {
		log.Infof("[Health Check][Check]client %s has been removed from callback", clientId)
		return nil
	}
	clientValue.mutex.Lock()
	defer clientValue.mutex.Unlock()
	var checkResp *plugin.CheckResponse
	var err error
	cachedClient := server.cacheProvider.GetClient(clientId)
	if cachedClient == nil {
		log.Infof("[Health Check][Check]client %s has been deleted", clientValue.id)
		return clientValue
	}
	request := &plugin.CheckRequest{
		QueryRequest: plugin.QueryRequest{
			InstanceId: toClientId(clientValue.id),
			Host:       clientValue.host,
			Port:       clientValue.port,
			Healthy:    true,
		},
		CurTimeSec:        currentTimeSec,
		ExpireDurationSec: clientValue.expireDurationSec,
	}
	checkResp, err = clientValue.checker.Check(request)
	if err != nil {
		log.Errorf("[Health Check][Check]fail to check client %s, id is %s, err is %v",
			clientValue.host, clientValue.id, err)
		return clientValue
	}
	if !checkResp.StayUnchanged {
		if !checkResp.Healthy {
			log.Infof(
				"[Health Check][Check]client change from healthy to unhealthy, id is %s, address is %s",
				clientValue.id, clientValue.host)
			code := asyncDeleteClient(cachedClient.Proto())
			if code != apimodel.Code_ExecuteSuccess {
				log.Errorf("[Health Check][Check]fail to update client, id is %s, address is %s, code is %d",
					clientValue.id, clientValue.host, code)
			}
		}
	}
	return clientValue
}

func (c *CheckScheduler) checkCallbackInstance(value interface{}) {
	instanceId := value.(string)
	instanceValue, ok := c.getInstanceValue(instanceId)
	if !ok {
		log.Infof("[Health Check][Check]instance %s has been removed from callback", instanceId)
		return
	}

	instanceValue.mutex.Lock()
	defer instanceValue.mutex.Unlock()

	var (
		checkResp *plugin.CheckResponse
		err       error
	)
	defer func() {
		if checkResp != nil && checkResp.Regular && checkResp.Healthy {
			c.addHealthyCallback(instanceValue, checkResp.LastHeartbeatTimeSec)
		} else {
			c.addUnHealthyCallback(instanceValue)
		}
	}()

	cachedInstance := server.cacheProvider.GetInstance(instanceId)
	if cachedInstance == nil {
		log.Infof("[Health Check][Check]instance %s has been deleted", instanceValue.id)
		return
	}
	request := &plugin.CheckRequest{
		QueryRequest: plugin.QueryRequest{
			InstanceId: instanceValue.id,
			Host:       instanceValue.host,
			Port:       instanceValue.port,
			Healthy:    cachedInstance.Healthy(),
		},
		CurTimeSec:        currentTimeSec,
		ExpireDurationSec: instanceValue.expireDurationSec,
	}
	checkResp, err = instanceValue.checker.Check(request)
	if err != nil {
		log.Errorf("[Health Check][Check]fail to check instance %s:%d, id is %s, err is %v",
			instanceValue.host, instanceValue.port, instanceValue.id, err)
		return
	}
	if !checkResp.StayUnchanged {
		code := setInsDbStatus(cachedInstance, checkResp.Healthy, checkResp.LastHeartbeatTimeSec)
		if checkResp.Healthy {
			// from unhealthy to healthy
			log.Infof(
				"[Health Check][Check]instance change from unhealthy to healthy, id is %s, address is %s:%d",
				instanceValue.id, instanceValue.host, instanceValue.port)
		} else {
			// from healthy to unhealthy
			log.Infof(
				"[Health Check][Check]instance change from healthy to unhealthy, id is %s, address is %s:%d",
				instanceValue.id, instanceValue.host, instanceValue.port)
		}
		if code != apimodel.Code_ExecuteSuccess {
			log.Errorf(
				"[Health Check][Check]fail to update instance, id is %s, address is %s:%d, code is %d",
				instanceValue.id, instanceValue.host, instanceValue.port, code)
		}
	}
}

// DelClient del client from check
func (c *CheckScheduler) DelClient(clientWithChecker *ClientWithChecker) {
	client := clientWithChecker.client
	clientId := client.Proto().GetId().GetValue()
	exists := c.delClientIfPresent(clientId)
	log.Infof("[Health Check][Check]remove check client is %s:%d, id is %s, exists is %v",
		client.Proto().GetHost().GetValue(), 0, clientId, exists)
	if exists {
		c.removeAdopting(clientId, clientWithChecker.checker)
	}
}

// DelInstance del instance from check
func (c *CheckScheduler) DelInstance(instanceWithChecker *InstanceWithChecker) {
	instance := instanceWithChecker.instance
	instanceId := instance.ID()
	exists := c.delInstanceIfPresent(instanceId)
	log.Infof("[Health Check][Check]remove check instance is %s:%d, id is %s, exists is %v",
		instance.Host(), instance.Port(), instanceId, exists)
	if exists {
		c.removeAdopting(instanceId, instanceWithChecker.checker)
	}
}

func (c *CheckScheduler) delInstanceIfPresent(instanceId string) bool {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	_, ok := c.scheduledInstances[instanceId]
	delete(c.scheduledInstances, instanceId)
	return ok
}

func (c *CheckScheduler) delClientIfPresent(clientId string) bool {
	c.rwMutex.Lock()
	defer c.rwMutex.Unlock()
	_, ok := c.scheduledClients[clientId]
	delete(c.scheduledClients, clientId)
	return ok
}

func (c *CheckScheduler) doCheckClient(ctx context.Context) {
	log.Infof("[Health Check][Check]client check worker has been started, tick seconds is %d",
		c.clientCheckIntervalSec)
	tick := time.NewTicker(time.Duration(c.clientCheckIntervalSec*1000+int64(getRandDelayMilli())) * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			var itemsToCheck []string
			if len(c.scheduledClients) == 0 {
				continue
			}
			curTimeSec := currentTimeSec()
			c.rwMutex.RLock()
			for id, value := range c.scheduledClients {
				if value.lastCheckTimeSec == 0 {
					itemsToCheck = append(itemsToCheck, id)
				}
				diff := curTimeSec - value.lastCheckTimeSec
				if diff < 0 || diff >= int64(value.expireDurationSec) {
					itemsToCheck = append(itemsToCheck, id)
				}
			}
			c.rwMutex.RUnlock()
			if len(itemsToCheck) == 0 {
				continue
			}
			for _, id := range itemsToCheck {
				item := c.checkCallbackClient(id)
				if nil != item {
					item.lastCheckTimeSec = currentTimeSec()
				}
			}
			timeCost := currentTimeSec() - curTimeSec
			log.Infof("[Health Check][Check]client check finished, time cost %d, client count %d",
				timeCost, len(itemsToCheck))
		case <-ctx.Done():
			log.Infof("[Health Check][Check]client check worker has been stopped")
			return
		}
	}
}

// setInsDbStatus 修改实例状态, 需要打印操作记录
func setInsDbStatus(instance *model.Instance, healthStatus bool, lastBeatTime int64) apimodel.Code {
	id := instance.ID()
	host := instance.Host()
	port := instance.Port()
	log.Infof("[Health Check][Check]addr:%s:%d id:%s set db status %v", host, port, id, healthStatus)

	var code apimodel.Code
	if server.bc.HeartbeatOpen() {
		code = asyncSetInsDbStatus(instance.Proto, healthStatus, lastBeatTime)
	} else {
		code = serialSetInsDbStatus(instance.Proto, healthStatus, lastBeatTime)
	}
	if code != apimodel.Code_ExecuteSuccess {
		return code
	}

	// 这里为了避免多次发送重复的事件，对实例原本的health 状态以及 healthStatus 状态进行对比，不一致才
	// 发布服务实例变更事件
	if instance.Healthy() != healthStatus {
		event := model.InstanceEvent{
			Id:        id,
			Namespace: instance.Namespace(),
			Service:   instance.Service(),
			Instance:  instance.Proto,
		}

		// 实例状态变化进行 DiscoverEvent 输出
		if healthStatus {
			event.EType = model.EventInstanceTurnHealth
		} else {
			event.EType = model.EventInstanceTurnUnHealth
		}

		server.publishInstanceEvent(instance.ServiceID, event)
	}

	return code
}

// asyncDeleteClient 异步软删除客户端
// 底层函数会合并delete请求，增加并发创建的吞吐
// req 原始请求
// ins 包含了req数据与instanceID，serviceToken
func asyncDeleteClient(client *apiservice.Client) apimodel.Code {
	future := server.bc.AsyncDeregisterClient(client)
	if err := future.Wait(); err != nil {
		log.Error("[Health Check][Check] async delete client", zap.String("client-id", client.GetId().GetValue()),
			zap.Error(err))
	}
	return future.Code()
}

// asyncSetInsDbStatus 异步新建实例
// 底层函数会合并delete请求，增加并发创建的吞吐
// req 原始请求
// ins 包含了req数据与instanceID，serviceToken
func asyncSetInsDbStatus(ins *apiservice.Instance, healthStatus bool, lastBeatTime int64) apimodel.Code {
	future := server.bc.AsyncHeartbeat(ins, healthStatus, lastBeatTime)
	if err := future.Wait(); err != nil {
		log.Error(err.Error())
	}
	return future.Code()
}

// serialSetInsDbStatus 同步串行创建实例
// req为原始的请求体
// ins包括了req的内容，并且填充了instanceID与serviceToken
func serialSetInsDbStatus(ins *apiservice.Instance, healthStatus bool, lastBeatTime int64) apimodel.Code {
	id := ins.GetId().GetValue()
	err := server.storage.SetInstanceHealthStatus(id, model.StatusBoolToInt(healthStatus), utils.NewUUID())
	if err != nil {
		log.Errorf("[Health Check][Check]id: %s set db status err:%s", id, err)
		return commonstore.StoreCode2APICode(err)
	}
	if healthStatus {
		if err := server.storage.BatchRemoveInstanceMetadata([]*store.InstanceMetadataRequest{
			{
				InstanceID: id,
				Revision:   utils.NewUUID(),
				Keys:       []string{model.MetadataInstanceLastHeartbeatTime},
			},
		}); err != nil {
			log.Errorf("[Batch] batch healthy check instances remove metadata err: %s", err.Error())
			return commonstore.StoreCode2APICode(err)
		}
	} else {
		if err := server.storage.BatchAppendInstanceMetadata([]*store.InstanceMetadataRequest{
			{
				InstanceID: id,
				Revision:   utils.NewUUID(),
				Metadata: map[string]string{
					model.MetadataInstanceLastHeartbeatTime: strconv.FormatInt(lastBeatTime, 10),
				},
			},
		}); err != nil {
			log.Errorf("[Batch] batch healthy check instances append metadata err: %s", err.Error())
			return commonstore.StoreCode2APICode(err)
		}
	}
	return apimodel.Code_ExecuteSuccess
}
