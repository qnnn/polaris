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

package sqldb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	apiservice "github.com/polarismesh/specification/source/go/api/v1/service_manage"
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/store"
)

// doubleWrite 双写health_check、instance_metadata开关
var doubleWrite = atomic.NewBool(true)

// instanceStore 实现了InstanceStore接口
type instanceStore struct {
	master *BaseDB // 大部分操作都用主数据库
	slave  *BaseDB // 缓存相关的读取，请求到slave
}

// AddInstance 添加实例
func (ins *instanceStore) AddInstance(instance *model.Instance) error {
	err := RetryTransaction("addInstance", func() error {
		return ins.addInstance(instance)
	})
	return store.Error(err)
}

// addInstance
func (ins *instanceStore) addInstance(instance *model.Instance) error {
	tx, err := ins.master.Begin()
	if err != nil {
		log.Errorf("[Store][database] add instance tx begin err: %s", err.Error())
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// 新增数据之前，必须先清理老数据
	if err := cleanInstance(tx, instance.ID()); err != nil {
		return err
	}

	if err := batchAddMainInstances(tx, []*model.Instance{instance}); err != nil {
		log.Errorf("[Store][database] add instance insert err: %s", err.Error())
		return err
	}

	if err := batchUpdateInstanceCheckV1IfNecessary(tx, []*model.Instance{instance}); err != nil {
		log.Errorf("[Store][database] double write instance check err: %s", err.Error())
		return err
	}

	if err := batchUpdateInstanceMetaV1IfNecessary(tx, []*model.Instance{instance}); err != nil {
		log.Errorf("[Store][database] double write instance meta err: %s", err.Error())
		return err
	}

	if err := tx.Commit(); err != nil {
		log.Errorf("[Store][database] add instance commit tx err: %s", err.Error())
		return err
	}

	return nil
}

// BatchAddInstances 批量增加实例
func (ins *instanceStore) BatchAddInstances(instances []*model.Instance) error {

	err := RetryTransaction("batchAddInstances", func() error {
		return ins.batchAddInstances(instances)
	})
	return store.Error(err)
}

// batchAddInstances batch add instances
func (ins *instanceStore) batchAddInstances(instances []*model.Instance) error {
	tx, err := ins.master.Begin()
	if err != nil {
		log.Errorf("[Store][database] batch add instances begin tx err: %s", err.Error())
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := batchAddMainInstances(tx, instances); err != nil {
		log.Errorf("[Store][database] batch add main instances err: %s", err.Error())
		return err
	}
	if err := batchUpdateInstanceCheckV1IfNecessary(tx, instances); err != nil {
		log.Errorf("[Store][database] double write instance check err: %s", err.Error())
		return err
	}
	if err := batchUpdateInstanceMetaV1IfNecessary(tx, instances); err != nil {
		log.Errorf("[Store][database] double write instance meta err: %s", err.Error())
		return err
	}

	if err := tx.Commit(); err != nil {
		log.Errorf("[Store][database] batch add instance commit tx err: %s", err.Error())
		return err
	}

	return nil
}

// UpdateInstance 更新实例
func (ins *instanceStore) UpdateInstance(instance *model.Instance) error {
	err := RetryTransaction("updateInstance", func() error {
		return ins.updateInstance(instance)
	})
	if err == nil {
		return nil
	}

	serr := store.Error(err)
	if store.Code(serr) == store.DuplicateEntryErr {
		serr = store.NewStatusError(store.DataConflictErr, err.Error())
	}
	return serr
}

// updateInstance update instance
func (ins *instanceStore) updateInstance(instance *model.Instance) error {
	tx, err := ins.master.Begin()
	if err != nil {
		log.Errorf("[Store][database] update instance tx begin err: %s", err.Error())
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// 更新main表
	if err := updateInstanceMain(tx, instance); err != nil {
		log.Errorf("[Store][database] update instance err: %s", err.Error())
		return err
	}
	// 更新health check表
	if err := batchUpdateInstanceCheckV1IfNecessary(tx, []*model.Instance{instance}); err != nil {
		log.Errorf("[Store][database] double write instance check err: %s", err.Error())
		return err
	}
	// 更新meta表
	if err := batchUpdateInstanceMetaV1IfNecessary(tx, []*model.Instance{instance}); err != nil {
		log.Errorf("[Store][database] double write instance meta err: %s", err.Error())
		return err
	}

	if err := tx.Commit(); err != nil {
		log.Errorf("[Store][database] update instance commit tx err: %s", err.Error())
		return err
	}

	return nil
}

// CleanInstance 清理数据
func (ins *instanceStore) CleanInstance(instanceID string) error {
	return RetryTransaction("cleanInstance", func() error {
		return ins.master.processWithTransaction("cleanInstance", func(tx *BaseTx) error {
			if err := cleanInstance(tx, instanceID); err != nil {
				return err
			}

			if err := tx.Commit(); err != nil {
				log.Errorf("[Store][database] clean instance commit tx err: %s", err.Error())
				return err
			}

			return nil
		})
	})
}

// cleanInstance 清理数据
func cleanInstance(tx *BaseTx, instanceID string) error {
	log.Infof("[Store][database] clean instance(%s)", instanceID)
	cleanIns := "delete from instance where id = ? and flag = 1"
	if _, err := tx.Exec(cleanIns, instanceID); err != nil {
		log.Errorf("[Store][database] clean instance(%s), err: %s", instanceID, err.Error())
		return store.Error(err)
	}
	cleanMeta := "delete from instance_metadata where id = ?"
	if _, err := tx.Exec(cleanMeta, instanceID); err != nil {
		log.Errorf("[Store][database] clean instance_metadata(%s), err: %s", instanceID, err.Error())
		return store.Error(err)
	}
	cleanCheck := "delete from health_check where id = ?"
	if _, err := tx.Exec(cleanCheck, instanceID); err != nil {
		log.Errorf("[Store][database] clean health_check(%s), err: %s", instanceID, err.Error())
		return store.Error(err)
	}
	return nil
}

// DeleteInstance 删除一个实例，删除实例实际上是把flag置为1
func (ins *instanceStore) DeleteInstance(instanceID string) error {
	if instanceID == "" {
		return errors.New("delete Instance Missing instance id")
	}
	return RetryTransaction("deleteInstance", func() error {
		return ins.master.processWithTransaction("deleteInstance", func(tx *BaseTx) error {
			str := "update instance set flag = 1, mtime = sysdate() where `id` = ?"
			if _, err := tx.Exec(str, instanceID); err != nil {
				return store.Error(err)
			}

			if err := tx.Commit(); err != nil {
				log.Errorf("[Store][database] delete instance commit tx err: %s", err.Error())
				return err
			}

			return nil
		})
	})
}

// BatchDeleteInstances 批量删除实例
func (ins *instanceStore) BatchDeleteInstances(ids []interface{}) error {
	return RetryTransaction("batchDeleteInstance", func() error {
		return ins.master.processWithTransaction("batchDeleteInstance", func(tx *BaseTx) error {
			if err := BatchOperation("delete-instance", ids, func(objects []interface{}) error {
				if len(objects) == 0 {
					return nil
				}
				str := `update instance set flag = 1, mtime = sysdate() where id in ( ` + PlaceholdersN(len(objects)) + `)`
				_, err := tx.Exec(str, objects...)
				return store.Error(err)
			}); err != nil {
				return err
			}

			if err := tx.Commit(); err != nil {
				log.Errorf("[Store][database] batch delete instance commit tx err: %s", err.Error())
				return err
			}

			return nil
		})
	})
}

// GetInstance 获取单个实例详情，只返回有效的数据
func (ins *instanceStore) GetInstance(instanceID string) (*model.Instance, error) {
	instance, err := ins.getInstance(instanceID)
	if err != nil {
		return nil, err
	}

	// 如果实例无效，则不返回
	if instance != nil && !instance.Valid {
		return nil, nil
	}

	return instance, nil
}

// BatchGetInstanceIsolate 检查实例是否存在
func (ins *instanceStore) BatchGetInstanceIsolate(ids map[string]bool) (map[string]bool, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	str := "select id, isolate from instance where flag = 0 and id in(" + PlaceholdersN(len(ids)) + ")"
	args := make([]interface{}, 0, len(ids))
	for key := range ids {
		args = append(args, key)
	}
	instanceIsolate := make(map[string]bool, len(ids))
	rows, err := ins.master.Query(str, args...)
	if err != nil {
		log.Errorf("[Store][database] check instances existed query err: %s", err.Error())
		return nil, err
	}
	var idx string
	var isolate int
	for rows.Next() {
		if err := rows.Scan(&idx, &isolate); err != nil {
			log.Errorf("[Store][database] check instances existed scan err: %s", err.Error())
			return nil, err
		}
		instanceIsolate[idx] = model.Int2bool(isolate)
	}

	return instanceIsolate, nil
}

// GetInstancesBrief 批量获取实例的serviceID
func (ins *instanceStore) GetInstancesBrief(ids map[string]bool) (map[string]*model.Instance, error) {
	if len(ids) == 0 {
		return nil, nil
	}

	str := `select instance.id, host, port, name, namespace, token, IFNULL(platform_id,"") from service, instance
		 where instance.flag = 0 and service.flag = 0 
		 and service.id = instance.service_id and instance.id in (` + PlaceholdersN(len(ids)) + ")"
	args := make([]interface{}, 0, len(ids))
	for key := range ids {
		args = append(args, key)
	}

	rows, err := ins.master.Query(str, args...)
	if err != nil {
		log.Errorf("[Store][database] get instances service token query err: %s", err.Error())
		return nil, err
	}
	defer rows.Close()

	out := make(map[string]*model.Instance, len(ids))
	var item model.ExpandInstanceStore
	var instance model.InstanceStore
	item.ServiceInstance = &instance
	for rows.Next() {
		if err := rows.Scan(&instance.ID, &instance.Host, &instance.Port,
			&item.ServiceName, &item.Namespace, &item.ServiceToken, &item.ServicePlatformID); err != nil {
			log.Errorf("[Store][database] get instances service token scan err: %s", err.Error())
			return nil, err
		}

		out[instance.ID] = model.ExpandStore2Instance(&item)
	}

	return out, nil
}

// GetInstancesCount 获取有效的实例总数
func (ins *instanceStore) GetInstancesCount() (uint32, error) {
	countStr := "select count(*) from instance where flag = 0"
	var count uint32
	var err error
	Retry("query-instance-row", func() error {
		err = ins.master.QueryRow(countStr).Scan(&count)
		return err
	})
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		log.Errorf("[Store][database] get instances count scan err: %s", err.Error())
		return 0, err
	default:
	}

	return count, nil
}

// GetInstancesCountTx .
func (ins *instanceStore) GetInstancesCountTx(tx store.Tx) (uint32, error) {
	dbTx, _ := tx.GetDelegateTx().(*BaseTx)
	countStr := "select count(*) from instance where flag = 0"
	var count uint32
	var err error
	Retry("query-instance-row", func() error {
		err = dbTx.QueryRow(countStr).Scan(&count)
		return err
	})
	switch {
	case err == sql.ErrNoRows:
		return 0, nil
	case err != nil:
		log.Errorf("[Store][database] get instances count scan err: %s", err.Error())
		return 0, err
	default:
	}

	return count, nil
}

// GetInstancesMainByService 根据服务和host获取实例
// @note 不包括metadata
func (ins *instanceStore) GetInstancesMainByService(serviceID, host string) ([]*model.Instance, error) {
	// 只查询有效的服务实例
	str := genInstanceSelectSQLWithoutMeta() + " where service_id = ? and host = ? and flag = 0"
	rows, err := ins.master.Query(str, serviceID, host)
	if err != nil {
		log.Errorf("[Store][database] get instances main query err: %s", err.Error())
		return nil, err
	}

	var out []*model.Instance
	err = callFetchInstanceWithoutMetaRows(rows, func(entry *model.InstanceStore) (b bool, e error) {
		out = append(out, model.Store2Instance(entry))
		return true, nil
	})
	if err != nil {
		log.Errorf("[Store][database] call fetch instance rows err: %s", err.Error())
		return nil, err
	}

	return out, nil
}

// GetExpandInstances 根据过滤条件查看对应服务实例及数目
// @note 暂不支持metadata过滤，instance表结构变更后，若要根据metadata过滤，需要遍历全量数据
func (ins *instanceStore) GetExpandInstances(filter, metaFilter map[string]string, offset uint32,
	limit uint32) (uint32, []*model.Instance, error) {
	// 只查询有效的实例列表
	filter["instance.flag"] = "0"

	out, err := ins.getExpandInstances(filter, metaFilter, offset, limit)
	if err != nil {
		return 0, nil, err
	}

	num, err := ins.getExpandInstancesCount(filter, metaFilter)
	if err != nil {
		return 0, nil, err
	}
	return num, out, err
}

// getExpandInstances 根据过滤条件查看对应服务实例
func (ins *instanceStore) getExpandInstances(filter, metaFilter map[string]string, offset uint32,
	limit uint32) ([]*model.Instance, error) {
	// 这种情况意味着，不需要详细的数据，可以不用query了
	if limit == 0 {
		return make([]*model.Instance, 0), nil
	}
	_, isName := filter["name"]
	_, isNamespace := filter["namespace"]
	_, isHost := filter["host"]
	needForceIndex := isName || isNamespace || isHost

	str := genExpandInstanceSelectSQL(needForceIndex)
	order := &Order{"instance.mtime", "desc"}
	str, args := genWhereSQLAndArgs(str, filter, metaFilter, order, offset, limit)

	rows, err := ins.master.Query(str, args...)
	if err != nil {
		log.Errorf("[Store][database] get instance by filters query err: %s, str: %s, args: %v", err.Error(), str, args)
		return nil, err
	}

	out, err := ins.getRowExpandInstances(rows)
	if err != nil {
		log.Errorf("[Store][database] get row instances err: %s", err.Error())
		return nil, err
	}

	return out, nil
}

// getExpandInstancesCount 根据过滤条件查看对应服务实例的数目
func (ins *instanceStore) getExpandInstancesCount(filter, metaFilter map[string]string) (uint32, error) {
	str := `select count(*) from instance `
	// 查询条件是否有service表中的字段
	_, isName := filter["name"]
	_, isNamespace := filter["namespace"]
	if isName || isNamespace {
		str += `inner join service on instance.service_id = service.id `
	}
	str, args := genWhereSQLAndArgs(str, filter, metaFilter, nil, 0, 1)

	var count uint32
	var err error
	Retry("query-instance-row", func() error {
		err = ins.master.QueryRow(str, args...).Scan(&count)
		return err
	})
	switch {
	case err == sql.ErrNoRows:
		log.Errorf("[Store][database] no row with this expand instance filter")
		return count, err
	case err != nil:
		log.Errorf("[Store][database] get expand instance count by filter err: %s", err.Error())
		return count, err
	default:
		return count, nil
	}
}

// GetMoreInstances 根据mtime获取增量修改数据
// 这里会返回所有的数据的，包括valid=false的数据
// 对于首次拉取，firstUpdate=true，只会拉取flag!=1的数据
func (ins *instanceStore) GetMoreInstances(tx store.Tx, mtime time.Time, firstUpdate, needMeta bool,
	serviceID []string) (map[string]*model.Instance, error) {

	dbTx, _ := tx.GetDelegateTx().(*BaseTx)
	if needMeta {
		instances, err := ins.getMoreInstancesMainWithMeta(dbTx, mtime, firstUpdate, serviceID)
		if err != nil {
			return nil, err
		}
		return instances, nil
	}
	instances, err := ins.getMoreInstancesMainWithoutMeta(dbTx, mtime, firstUpdate, serviceID)
	if err != nil {
		return nil, err
	}
	return instances, nil
}

// GetInstanceMeta 根据实例ID获取实例的metadata
func (ins *instanceStore) GetInstanceMeta(instanceID string) (map[string]string, error) {
	instance, err := ins.GetInstance(instanceID)
	if err != nil {
		log.Errorf("[Store][database] query instance meta err: %s", err.Error())
		return nil, err
	}
	if instance == nil || instance.Metadata() == nil {
		return map[string]string{}, nil
	}
	return instance.Metadata(), err
}

// SetInstanceHealthStatus 设置实例健康状态
func (ins *instanceStore) SetInstanceHealthStatus(instanceID string, flag int, revision string) error {
	return RetryTransaction("setInstanceHealthStatus", func() error {
		return ins.master.processWithTransaction("setInstanceHealthStatus", func(tx *BaseTx) error {
			str := "update instance set health_status = ?, revision = ?, mtime = sysdate() where `id` = ?"
			if _, err := tx.Exec(str, flag, revision, instanceID); err != nil {
				return store.Error(err)
			}

			if err := tx.Commit(); err != nil {
				log.Errorf("[Store][database] set instance health status commit tx err: %s", err.Error())
				return err
			}

			return nil
		})
	})
}

// BatchSetInstanceHealthStatus 批量设置健康状态
func (ins *instanceStore) BatchSetInstanceHealthStatus(ids []interface{}, isolate int, revision string) error {
	return RetryTransaction("batchSetInstanceHealthStatus", func() error {
		return ins.master.processWithTransaction("batchSetInstanceHealthStatus", func(tx *BaseTx) error {
			if err := BatchOperation("set-instance-healthy", ids, func(objects []interface{}) error {
				if len(objects) == 0 {
					return nil
				}
				str := "update instance set health_status = ?, revision = ?, mtime = sysdate() where id in "
				str += "(" + PlaceholdersN(len(objects)) + ")"
				args := make([]interface{}, 0, len(objects)+2)
				args = append(args, isolate)
				args = append(args, revision)
				args = append(args, objects...)
				_, err := tx.Exec(str, args...)
				return store.Error(err)
			}); err != nil {
				return err
			}

			if err := tx.Commit(); err != nil {
				log.Errorf("[Store][database] batch set instance health status commit tx err: %s", err.Error())
				return err
			}

			return nil
		})
	})
}

// BatchSetInstanceIsolate 批量设置实例隔离状态
func (ins *instanceStore) BatchSetInstanceIsolate(ids []interface{}, isolate int, revision string) error {
	return RetryTransaction("batchSetInstanceIsolate", func() error {
		return ins.master.processWithTransaction("batchSetInstanceIsolate", func(tx *BaseTx) error {
			if err := BatchOperation("set-instance-isolate", ids, func(objects []interface{}) error {
				if len(objects) == 0 {
					return nil
				}
				str := "update instance set isolate = ?, revision = ?, mtime = sysdate() where id in "
				str += "(" + PlaceholdersN(len(objects)) + ")"
				args := make([]interface{}, 0, len(objects)+2)
				args = append(args, isolate)
				args = append(args, revision)
				args = append(args, objects...)
				_, err := tx.Exec(str, args...)
				return store.Error(err)
			}); err != nil {
				return err
			}

			if err := tx.Commit(); err != nil {
				log.Errorf("[Store][database] batch set instance isolate commit tx err: %s", err.Error())
				return err
			}

			return nil
		})
	})
}

// BatchAppendInstanceMetadata 追加实例 metadata
func (ins *instanceStore) BatchAppendInstanceMetadata(requests []*store.InstanceMetadataRequest) error {
	if len(requests) == 0 {
		return nil
	}
	return ins.master.processWithTransaction("AppendInstanceMetadata", func(tx *BaseTx) error {
		for i := range requests {
			id := requests[i].InstanceID
			revision := requests[i].Revision
			appendMetadata := requests[i].Metadata
			instance, err := ins.GetInstance(id)
			if err != nil {
				log.Errorf("[Store][database] append instance metadata query source instance err: %s", err.Error())
				return err
			}
			var sourceMetadata map[string]string
			if instance == nil || instance.Metadata() == nil {
				sourceMetadata = make(map[string]string, len(appendMetadata))
			} else {
				sourceMetadata = instance.Metadata()
			}
			for metaKey, metaVal := range appendMetadata {
				sourceMetadata[metaKey] = metaVal
			}
			if instance != nil {
				instance.Proto.Metadata = sourceMetadata
			}

			metadataJson, err := marshalInstanceMetadata(sourceMetadata)
			if err != nil {
				log.Errorf("[Store][database] append instance metadata err: %s", err.Error())
				return err
			}
			str := "update instance set revision = ?, metadata = ?, mtime = sysdate() where id = ?"
			if log.DebugEnabled() {
				log.Debug("[Store][database] append instance metadata",
					zap.String("instanceId", id), zap.Any("appendMeta", appendMetadata))
			}
			if _, err := tx.Exec(str, revision, metadataJson, id); err != nil {
				log.Errorf("[Store][database] append instance metadata update revision err: %s", err.Error())
				return err
			}

			if err := batchUpdateInstanceMetaV1IfNecessary(tx, []*model.Instance{instance}); err != nil {
				log.Errorf("[Store][database] double write instance meta err: %s", err.Error())
				return err
			}
		}
		return tx.Commit()
	})
}

// BatchRemoveInstanceMetadata 删除实例指定的 metadata
func (ins *instanceStore) BatchRemoveInstanceMetadata(requests []*store.InstanceMetadataRequest) error {
	if len(requests) == 0 {
		return nil
	}
	return ins.master.processWithTransaction("RemoveInstanceMetadata", func(tx *BaseTx) error {
		for i := range requests {
			id := requests[i].InstanceID
			revision := requests[i].Revision
			keys := requests[i].Keys
			instance, err := ins.GetInstance(id)
			if err != nil {
				log.Errorf("[Store][database] query instance metadata err: %s", err.Error())
				return err
			}
			var sourceMetadata map[string]string
			if instance == nil || instance.Metadata() == nil {
				sourceMetadata = make(map[string]string)
			} else {
				sourceMetadata = instance.Metadata()
			}
			for _, key := range keys {
				delete(sourceMetadata, key)
			}
			if instance != nil {
				instance.Proto.Metadata = sourceMetadata
			}
			metadataJson, err := marshalInstanceMetadata(sourceMetadata)
			if err != nil {
				log.Errorf("[Store][database] remove instance metadata err: %s", err.Error())
				return err
			}

			str := "update instance set revision = ?, metadata = ?, mtime = sysdate() where id = ?"
			if _, err := tx.Exec(str, revision, metadataJson, id); err != nil {
				log.Errorf("[Store][database] remove instance metadata by keys update revision err: %s", err.Error())
				return err
			}

			if err := batchUpdateInstanceMetaV1IfNecessary(tx, []*model.Instance{instance}); err != nil {
				log.Errorf("[Store][database] double write instance meta err: %s", err.Error())
				return err
			}
		}
		return tx.Commit()
	})
}

// getInstance 内部获取instance函数，根据instanceID，直接读取元数据，不做其他过滤
func (ins *instanceStore) getInstance(instanceID string) (*model.Instance, error) {
	str := genInstanceSelectSQLWithMeta() + " where instance.id = ?"
	rows, err := ins.master.Query(str, instanceID)
	if err != nil {
		log.Errorf("[Store][database] get instance query err: %s", err.Error())
		return nil, err
	}

	out, err := fetchInstanceRows(rows)
	if err != nil {
		return nil, err
	}

	if len(out) == 0 {
		return nil, err
	}

	selectMetadataSql := "select id, mkey, mvalue from instance_metadata where id = ?"

	rows, err = ins.master.Query(selectMetadataSql, instanceID)
	if err != nil {
		log.Errorf("[Store][database] acquire instances meta query err: %s", err.Error())
		return nil, err
	}
	if err := fetchInstanceMetaRows(map[string]*model.Instance{out[0].ID(): out[0]}, rows); err != nil {
		return nil, err
	}

	return out[0], nil
}

// getMoreInstancesMainWithMeta 获取增量instance+healthcheck+meta内容
// @note ro库有多个实例，且主库到ro库各实例的同步时间不一致。为避免获取不到meta，需要采用一条sql语句获取全部数据
func (ins *instanceStore) getMoreInstancesMainWithMeta(tx *BaseTx, mtime time.Time, firstUpdate bool, serviceID []string) (
	map[string]*model.Instance, error) {
	// 增量获取数据时，若双写开关开启，此时处于升级过程中，需要处理note中的问题
	if !firstUpdate && doubleWrite.Load() {
		instances, err := fetchInstanceMetaInSingleSQL(tx, mtime, serviceID)
		if err != nil {
			return nil, err
		}
		return instances, err
	}
	instances, err := ins.getMoreInstancesMain(tx, mtime, firstUpdate, serviceID)
	if err != nil {
		log.Errorf("[Store][database] get more instance main err: %s", err.Error())
		return nil, err
	}
	if len(instances) == 0 {
		return instances, err
	}

	var rows *sql.Rows
	if !firstUpdate {
		data := make([]interface{}, 0, len(instances))
		for idx := range instances {
			data = append(data, instances[idx].Proto)
		}
		rows, err = batchQueryMetadata(tx.Query, data)
	} else {
		str := "select `id`, `mkey`, `mvalue` from instance_metadata"
		rows, err = tx.Query(str)
	}
	if err != nil {
		log.Errorf("[Store][database] instances meta query err: %s", err.Error())
		return nil, err
	}

	if err := fetchInstanceMetaRows(instances, rows); err != nil {
		return nil, err
	}
	return instances, nil
}

// fetchInstanceMetaInSingleSQL 在一条sql中获取增量instance+healthcheck+meta内容
func fetchInstanceMetaInSingleSQL(tx *BaseTx, mtime time.Time, serviceID []string) (
	map[string]*model.Instance, error) {
	str := genCompleteInstanceSelectSQL() + " where instance.mtime >= FROM_UNIXTIME(?)"
	args := make([]interface{}, 0, len(serviceID)+1)
	args = append(args, timeToTimestamp(mtime))

	if len(serviceID) > 0 {
		str += " and service_id in (" + PlaceholdersN(len(serviceID))
		str += ")"
	}
	for _, id := range serviceID {
		args = append(args, id)
	}
	rows, err := tx.Query(str, args...)
	if err != nil {
		log.Errorf("[Store][database] get more instance query err: %s", err.Error())
		return nil, err
	}
	return fetchInstanceWithMetaRows(rows)
}

// fetchInstanceWithMetaRows 获取instance main+health_check+instance_metadata rows里面的数据
func fetchInstanceWithMetaRows(rows *sql.Rows) (map[string]*model.Instance, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	out := make(map[string]*model.Instance)
	var item model.InstanceStore
	var metadataStr, id, mKey, mValue string
	// 存储instance与metadata表的映射关系，用于覆盖instance表中metadata字段。结构为：instanceId => map[string]string
	instanceMetaMap := make(map[string]map[string]string)
	progress := 0
	for rows.Next() {
		progress++
		if progress%100000 == 0 {
			log.Infof("[Store][database] instance+meta fetch rows progress: %d", progress)
		}
		if err := rows.Scan(&item.ID, &item.ServiceID, &item.VpcID, &item.Host, &item.Port, &item.Protocol,
			&item.Version, &item.HealthStatus, &item.Isolate, &item.Weight, &item.EnableHealthCheck, &item.LogicSet,
			&item.Region, &item.Zone, &item.Campus, &item.Priority, &item.Revision, &item.Flag, &item.CheckType,
			&item.TTL, &metadataStr, &id, &mKey, &mValue, &item.CreateTime, &item.ModifyTime); err != nil {
			log.Errorf("[Store][database] fetch instance+meta rows err: %s", err.Error())
			return nil, err
		}
		if _, ok := out[item.ID]; !ok {
			out[item.ID] = model.Store2Instance(&item)
		}
		// 已初始化过的不再重复初始化
		if len(metadataStr) != 0 && out[item.ID].Proto.Metadata == nil {
			metadata, err := unMarshalInstanceMetadata(metadataStr)
			if err != nil {
				return nil, err
			}
			out[item.ID].Proto.Metadata = metadata
		}
		// 实例存在meta
		if id != "" {
			if instanceMetaMap[item.ID] == nil {
				instanceMetaMap[item.ID] = make(map[string]string)
			}
			instanceMetaMap[item.ID][mKey] = mValue
		}
	}
	if err := rows.Err(); err != nil {
		log.Errorf("[Store][database] fetch instance+metadata rows next err: %s", err.Error())
		return nil, err
	}
	for instanceId, metadata := range instanceMetaMap {
		if _, ok := out[instanceId]; !ok {
			continue
		}
		out[instanceId].Proto.Metadata = metadata
	}
	return out, nil
}

// getMoreInstancesMainWithoutMeta 获取增量instance+healthcheck内容
func (ins *instanceStore) getMoreInstancesMainWithoutMeta(tx *BaseTx, mtime time.Time, firstUpdate bool, serviceID []string) (
	map[string]*model.Instance, error) {
	str := genInstanceSelectSQLWithoutMeta() + " where mtime >= FROM_UNIXTIME(?)"
	args := make([]interface{}, 0, len(serviceID)+1)
	args = append(args, timeToTimestamp(mtime))

	if firstUpdate {
		str += " and flag != 1"
	}

	if len(serviceID) > 0 {
		str += " and service_id in (" + PlaceholdersN(len(serviceID))
		str += ")"
	}
	for _, id := range serviceID {
		args = append(args, id)
	}

	rows, err := tx.Query(str, args...)
	if err != nil {
		log.Errorf("[Store][database] get more instance query err: %s", err.Error())
		return nil, err
	}

	out := make(map[string]*model.Instance)
	err = callFetchInstanceWithoutMetaRows(rows, func(entry *model.InstanceStore) (b bool, e error) {
		out[entry.ID] = model.Store2Instance(entry)
		return true, nil
	})
	if err != nil {
		log.Errorf("[Store][database] call fetch instance rows err: %s", err.Error())
		return nil, err
	}
	return out, nil
}

// getMoreInstancesMain 获取增量instances 主表内容，health_check内容，metadata内容
func (ins *instanceStore) getMoreInstancesMain(tx *BaseTx, mtime time.Time, firstUpdate bool,
	serviceID []string) (map[string]*model.Instance, error) {
	str := genInstanceSelectSQLWithMeta() + " where mtime >= FROM_UNIXTIME(?)"
	args := make([]interface{}, 0, len(serviceID)+1)
	args = append(args, timeToTimestamp(mtime))

	if firstUpdate {
		str += " and flag != 1"
	}

	if len(serviceID) > 0 {
		str += " and service_id in (" + PlaceholdersN(len(serviceID))
		str += ")"
	}
	for _, id := range serviceID {
		args = append(args, id)
	}

	rows, err := tx.Query(str, args...)
	if err != nil {
		log.Errorf("[Store][database] get more instance query err: %s", err.Error())
		return nil, err
	}

	out := make(map[string]*model.Instance)
	err = callFetchInstanceWithMetaRows(rows, func(entry *model.InstanceStore) (b bool, e error) {
		out[entry.ID] = model.Store2Instance(entry)
		return true, nil
	})
	if err != nil {
		log.Errorf("[Store][database] call fetch instance rows err: %s", err.Error())
		return nil, err
	}

	return out, nil
}

// getRowExpandInstances 根据rows获取对应expandInstance
func (ins *instanceStore) getRowExpandInstances(rows *sql.Rows) ([]*model.Instance, error) {
	out, err := fetchExpandInstanceRows(rows)
	if err != nil {
		return nil, err
	}

	data := make([]interface{}, 0, len(out))
	for idx := range out {
		data = append(data, out[idx].Proto)
	}

	err = BatchQuery("expand-instance-metadata", data, func(objects []interface{}) error {
		return ins.batchAcquireInstanceMetadata(objects)
	})
	if err != nil {
		log.Errorf("[Store][database] get expand instances metadata err: %s", err.Error())
		return nil, err
	}

	return out, nil
}

// batchAcquireInstanceMetadata 批量获取instance的metadata信息
// web端获取实例的数据的时候使用
func (ins *instanceStore) batchAcquireInstanceMetadata(instances []interface{}) error {
	rows, err := batchQueryMetadata(ins.master.Query, instances)
	if err != nil {
		return err
	}
	if rows == nil {
		return nil
	}
	defer rows.Close()

	out := make(map[string]map[string]string)
	var id, key, value string
	for rows.Next() {
		if err := rows.Scan(&id, &key, &value); err != nil {
			log.Errorf("[Store][database] multi query instance metadata rows scan err: %s", err.Error())
			return err
		}
		if _, ok := out[id]; !ok {
			out[id] = make(map[string]string)
		}
		out[id][key] = value
	}
	if err := rows.Err(); err != nil {
		log.Errorf("[Store][database] multi query instance metadata rows next err: %s", err.Error())
		return err
	}

	// 把接收到的metadata，设置到instance中
	for _, ele := range instances {
		instanceId := ele.(*apiservice.Instance).GetId().GetValue()
		metadata, ok := out[instanceId]
		if ok {
			ele.(*apiservice.Instance).Metadata = metadata
		}
	}

	return nil
}

// batchQueryMetadata 批量查找metadata
func batchQueryMetadata(queryHandler QueryHandler, instances []interface{}) (*sql.Rows, error) {
	if len(instances) == 0 {
		return nil, nil
	}

	str := "select `id`, `mkey`, `mvalue` from instance_metadata where id in("
	first := true
	args := make([]interface{}, 0, len(instances))
	for _, ele := range instances {
		if first {
			str += "?"
			first = false
		} else {
			str += ",?"
		}
		args = append(args, ele.(*apiservice.Instance).GetId().GetValue())
	}
	str += ")"

	rows, err := queryHandler(str, args...)
	if err != nil {
		log.Errorf("[Store][database] multi query instance metadata err: %s", err.Error())
		return nil, err
	}

	return rows, nil
}

// batchAddMainInstances 批量增加main instance数据，包括health_check、metadata
func batchAddMainInstances(tx *BaseTx, instances []*model.Instance) error {
	str := `replace into instance(id, service_id, vpc_id, host, port, protocol, version, health_status, isolate,
		 weight, enable_health_check, logic_set, cmdb_region, cmdb_zone, cmdb_idc, priority, revision, 
		 health_check_type, health_check_ttl, metadata, ctime, mtime) 
		 values`
	first := true
	args := make([]interface{}, 0)
	for _, entry := range instances {
		if !first {
			str += ","
		}
		str += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, sysdate(), sysdate())"
		first = false
		args = append(args, entry.ID(), entry.ServiceID, entry.VpcID(), entry.Host(), entry.Port())
		args = append(args, entry.Protocol(), entry.Version(), entry.Healthy(), entry.Isolate(),
			entry.Weight())
		args = append(args, entry.EnableHealthCheck(), entry.LogicSet(),
			entry.Location().GetRegion().GetValue(), entry.Location().GetZone().GetValue(),
			entry.Location().GetCampus().GetValue(), entry.Priority(), entry.Revision())
		if entry.HealthCheck() != nil {
			args = append(args, entry.HealthCheck().GetType(), entry.HealthCheck().GetHeartbeat().GetTtl().GetValue())
		} else {
			args = append(args, nil, nil)
		}
		if entry.Metadata() != nil {
			metadata, err := marshalInstanceMetadata(entry.Metadata())
			if err != nil {
				return err
			}
			args = append(args, metadata)
		} else {
			args = append(args, nil)
		}
	}
	_, err := tx.Exec(str, args...)
	return err
}

// batchDeleteInstanceMeta 批量删除metadata数据
func batchDeleteInstanceMeta(tx *BaseTx, instances []*model.Instance) error {
	ids := make([]interface{}, 0, len(instances))
	builder := strings.Builder{}
	for _, entry := range instances {
		// If instance is first registration, no need to participate in the following METADATA cleaning action
		// if entry.FirstRegis {
		// 	continue
		// }
		if entry == nil {
			continue
		}

		ids = append(ids, entry.ID())

		if len(ids) > 1 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}

	if len(ids) == 0 {
		return nil
	}

	str := `delete from instance_metadata where id in (` + builder.String() + `)`
	_, err := tx.Exec(str, ids...)
	return err
}

// batchAddInstanceMeta 批量增加metadata数据
func batchAddInstanceMeta(tx *BaseTx, instances []*model.Instance) error {
	str := "insert into instance_metadata(`id`, `mkey`, `mvalue`, `ctime`, `mtime`) values"
	args := make([]interface{}, 0)
	first := true
	for _, entry := range instances {
		if entry == nil || len(entry.Metadata()) == 0 {
			continue
		}

		for key, value := range entry.Metadata() {
			if !first {
				str += ","
			}
			str += "(?, ?, ?, sysdate(), sysdate())"
			first = false
			args = append(args, entry.ID(), key, value)
		}
	}
	// 不存在metadata，直接返回
	if first {
		return nil
	}

	if log.DebugEnabled() {
		log.Debug("[Store][database] double write instance meta", zap.String("sql", str), zap.Any("args", args))
	}

	_, err := tx.Exec(str, args...)
	return err
}

// updateInstanceMain 更新instance主表
func updateInstanceMain(tx *BaseTx, instance *model.Instance) error {
	str := `update instance set protocol = ?,
	 version = ?, health_status = ?, isolate = ?, weight = ?, enable_health_check = ?, logic_set = ?,
	 cmdb_region = ?, cmdb_zone = ?, cmdb_idc = ?, priority = ?, revision = ?, health_check_type = ?, 
	 health_check_ttl = ?,`

	// 只有metadata为nil的时候，则不用处理
	if instance.Metadata() != nil {
		str += ` metadata = ?,`
	}
	str += ` mtime = sysdate() where id = ?`

	args := make([]interface{}, 0)
	args = append(args, instance.Protocol(), instance.Version(), instance.Healthy(), instance.Isolate())
	args = append(args, instance.Weight(), instance.EnableHealthCheck(), instance.LogicSet())
	args = append(args, instance.Location().GetRegion().GetValue(), instance.Location().GetZone().GetValue())
	args = append(args, instance.Location().GetCampus().GetValue(), instance.Priority(), instance.Revision())

	if instance.HealthCheck() != nil {
		args = append(args, instance.HealthCheck().GetType(), instance.HealthCheck().GetHeartbeat().GetTtl().GetValue())
	} else {
		args = append(args, nil, nil)
	}

	if instance.Metadata() != nil {
		metadata, err := marshalInstanceMetadata(instance.Metadata())
		if err != nil {
			return err
		}
		args = append(args, metadata)
	}

	args = append(args, instance.ID())

	_, err := tx.Exec(str, args...)

	return err
}

// fetchInstanceRows 获取instance rows的内容
func fetchInstanceRows(rows *sql.Rows) ([]*model.Instance, error) {
	var out []*model.Instance
	err := callFetchInstanceWithMetaRows(rows, func(entry *model.InstanceStore) (b bool, e error) {
		out = append(out, model.Store2Instance(entry))
		return true, nil
	})
	if err != nil {
		log.Errorf("[Store][database] call fetch instance rows err: %s", err.Error())
		return nil, err
	}

	return out, nil
}

// callFetchInstanceWithoutMetaRows 带回调的fetch instance
func callFetchInstanceWithoutMetaRows(rows *sql.Rows, callback func(entry *model.InstanceStore) (bool, error)) error {
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var item model.InstanceStore
	progress := 0
	for rows.Next() {
		progress++
		if progress%100000 == 0 {
			log.Infof("[Store][database] instance fetch rows progress: %d", progress)
		}
		err := rows.Scan(&item.ID, &item.ServiceID, &item.VpcID, &item.Host, &item.Port, &item.Protocol,
			&item.Version, &item.HealthStatus, &item.Isolate, &item.Weight, &item.EnableHealthCheck,
			&item.LogicSet, &item.Region, &item.Zone, &item.Campus, &item.Priority, &item.Revision,
			&item.Flag, &item.CheckType, &item.TTL, &item.CreateTime, &item.ModifyTime)
		if err != nil {
			log.Errorf("[Store][database] fetch instance rows err: %s", err.Error())
			return err
		}
		ok, err := callback(&item)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		log.Errorf("[Store][database] instance rows catch err: %s", err.Error())
		return err
	}

	return nil
}

// callFetchInstanceWithMetaRows 带回调的fetch instance
func callFetchInstanceWithMetaRows(rows *sql.Rows, callback func(entry *model.InstanceStore) (bool, error)) error {
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var item model.InstanceStore
	var metadata string
	progress := 0
	for rows.Next() {
		progress++
		if progress%100000 == 0 {
			log.Infof("[Store][database] instance fetch rows progress: %d", progress)
		}
		err := rows.Scan(&item.ID, &item.ServiceID, &item.VpcID, &item.Host, &item.Port, &item.Protocol,
			&item.Version, &item.HealthStatus, &item.Isolate, &item.Weight, &item.EnableHealthCheck,
			&item.LogicSet, &item.Region, &item.Zone, &item.Campus, &item.Priority, &item.Revision,
			&item.Flag, &item.CheckType, &item.TTL, &metadata, &item.CreateTime, &item.ModifyTime)
		if err != nil {
			log.Errorf("[Store][database] fetch instance rows err: %s", err.Error())
			return err
		}
		item.Meta = nil
		if len(metadata) != 0 {
			item.Meta, err = unMarshalInstanceMetadata(metadata)
			if err != nil {
				return err
			}
		}

		ok, err := callback(&item)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		log.Errorf("[Store][database] instance rows catch err: %s", err.Error())
		return err
	}

	return nil
}

// fetchExpandInstanceRows 获取expandInstance rows的内容
func fetchExpandInstanceRows(rows *sql.Rows) ([]*model.Instance, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var out []*model.Instance
	var item model.ExpandInstanceStore
	var instance model.InstanceStore
	var metadataStr string
	item.ServiceInstance = &instance
	progress := 0
	for rows.Next() {
		progress++
		if progress%50000 == 0 {
			log.Infof("[Store][database] expand instance fetch rows progress: %d", progress)
		}
		err := rows.Scan(&instance.ID, &instance.ServiceID, &instance.VpcID, &instance.Host, &instance.Port,
			&instance.Protocol, &instance.Version, &instance.HealthStatus, &instance.Isolate,
			&instance.Weight, &instance.EnableHealthCheck, &instance.LogicSet, &instance.Region,
			&instance.Zone, &instance.Campus, &instance.Priority, &instance.Revision, &instance.Flag,
			&instance.CheckType, &instance.TTL, &metadataStr, &item.ServiceName, &item.Namespace,
			&instance.CreateTime, &instance.ModifyTime)
		if err != nil {
			log.Errorf("[Store][database] fetch instance rows err: %s", err.Error())
			return nil, err
		}
		instanceItem := model.ExpandStore2Instance(&item)
		if len(metadataStr) != 0 {
			instanceItem.Proto.Metadata, err = unMarshalInstanceMetadata(metadataStr)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, instanceItem)
	}

	if err := rows.Err(); err != nil {
		log.Errorf("[Store][database] instance rows catch err: %s", err.Error())
		return nil, err
	}

	return out, nil
}

// fetchInstanceMetaRows 解析获取instance metadata
func fetchInstanceMetaRows(instances map[string]*model.Instance, rows *sql.Rows) error {
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var id, key, value string
	// 存储instance与metadata表的映射关系，用于覆盖instance表中metadata字段。结构为：instanceId => map[string]string
	instanceMetaMap := make(map[string]map[string]string, len(instances))
	progress := 0
	for rows.Next() {
		progress++
		if progress%500000 == 0 {
			log.Infof("[Store][database] fetch instance metadata progress: %d", progress)
		}
		if err := rows.Scan(&id, &key, &value); err != nil {
			log.Errorf("[Store][database] fetch instance metadata rows scan err: %s", err.Error())
			return err
		}
		// 不在目标列表，不存储
		if _, ok := instances[id]; !ok {
			continue
		}
		if instanceMetaMap[id] == nil {
			instanceMetaMap[id] = make(map[string]string)
		}
		instanceMetaMap[id][key] = value
	}
	if err := rows.Err(); err != nil {
		log.Errorf("[Store][database] fetch instance metadata rows next err: %s", err.Error())
		return err
	}
	for instanceId, metadata := range instanceMetaMap {
		if _, ok := instances[instanceId]; !ok {
			continue
		}
		instances[instanceId].Proto.Metadata = metadata
	}
	return nil
}

// genInstanceSelectSQLWithMeta 生成instance的select sql语句，包括metadata
func genInstanceSelectSQLWithMeta() string {
	str := `select instance.id, service_id, IFNULL(vpc_id,""), host, port, IFNULL(protocol, ""), IFNULL(version, ""),
			 health_status, isolate, weight, enable_health_check, IFNULL(logic_set, ""), IFNULL(cmdb_region, ""), 
			 IFNULL(cmdb_zone, ""), IFNULL(cmdb_idc, ""), priority, revision, flag, 
			 IFNULL(health_check.type, IFNULL(health_check_type, -1)), IFNULL(health_check.ttl, IFNULL(health_check_ttl, 0)),
			 IFNULL(metadata, ""), UNIX_TIMESTAMP(ctime), UNIX_TIMESTAMP(mtime)   
			 from instance 
			 left join health_check on instance.id = health_check.id`
	return str
}

// genInstanceSelectSQLWithoutMeta 生成instance的select sql语句，不包括metadata
func genInstanceSelectSQLWithoutMeta() string {
	str := `select instance.id, service_id, IFNULL(vpc_id,""), host, port, IFNULL(protocol, ""), IFNULL(version, ""),
			 health_status, isolate, weight, enable_health_check, IFNULL(logic_set, ""), IFNULL(cmdb_region, ""), 
			 IFNULL(cmdb_zone, ""), IFNULL(cmdb_idc, ""), priority, revision, flag, 
			 IFNULL(health_check.type, IFNULL(health_check_type, -1)), IFNULL(health_check.ttl, IFNULL(health_check_ttl, 0)),
			 UNIX_TIMESTAMP(ctime), UNIX_TIMESTAMP(mtime)   
			 from instance 
			 left join health_check on instance.id = health_check.id`
	return str
}

// genCompleteInstanceSelectSQL 生成完整instance(主表+health_check+metadata)的sql语句
func genCompleteInstanceSelectSQL() string {
	str := `select instance.id, service_id, IFNULL(vpc_id,""), host, port, IFNULL(protocol, ""), IFNULL(version, ""),
			 health_status, isolate, weight, enable_health_check, IFNULL(logic_set, ""), IFNULL(cmdb_region, ""), 
			 IFNULL(cmdb_zone, ""), IFNULL(cmdb_idc, ""), priority, revision, flag, 
			 IFNULL(health_check.type, IFNULL(health_check_type, -1)), IFNULL(health_check.ttl, IFNULL(health_check_ttl, 0)),
			 IFNULL(metadata, ""), IFNULL(instance_metadata.id, ""), IFNULL(mkey, ""), IFNULL(mvalue, ""), 
			 UNIX_TIMESTAMP(instance.ctime), UNIX_TIMESTAMP(instance.mtime)
		     from instance 
		     left join health_check on instance.id = health_check.id 
		     left join instance_metadata on instance.id = instance_metadata.id `
	return str
}

// genExpandInstanceSelectSQL 生成expandInstance的select sql语句
func genExpandInstanceSelectSQL(needForceIndex bool) string {
	str := `select instance.id, service_id, IFNULL(vpc_id,""), host, port, IFNULL(protocol, ""), IFNULL(version, ""),
			 health_status, isolate, weight, enable_health_check, IFNULL(logic_set, ""), IFNULL(cmdb_region, ""), 
			 IFNULL(cmdb_zone, ""), IFNULL(cmdb_idc, ""), priority, instance.revision, instance.flag, 
			 IFNULL(health_check.type, IFNULL(health_check_type, -1)), IFNULL(health_check.ttl, IFNULL(health_check_ttl, 0)),
			 IFNULL(metadata, ""), service.name, service.namespace, 
			 UNIX_TIMESTAMP(instance.ctime), UNIX_TIMESTAMP(instance.mtime) 
			 from (service inner join instance `
	if needForceIndex {
		str += `force index(service_id, host) `
	}
	str += `on service.id = instance.service_id) left join health_check on instance.id = health_check.id `
	return str
}

// marshalInstanceMetadata 将metadata解析为json格式
func marshalInstanceMetadata(v any) (string, error) {
	metadata, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(metadata), err
}

// unMarshalInstanceMetadata 解析得到metadata
func unMarshalInstanceMetadata(meta string) (map[string]string, error) {
	metadata := make(map[string]string)
	err := json.Unmarshal([]byte(meta), &metadata)
	return metadata, err
}

// batchUpdateInstanceCheckV1IfNecessary 批量变更healthCheck数据
// @note 升级过程中双写，升级后关闭
func batchUpdateInstanceCheckV1IfNecessary(tx *BaseTx, instances []*model.Instance) error {
	ids := make([]interface{}, 0, len(instances))
	builder := strings.Builder{}
	str := "replace into health_check(`id`, `type`, `ttl`) values"
	first := true
	args := make([]interface{}, 0)
	for _, entry := range instances {
		ids = append(ids, entry.ID())
		if len(ids) > 1 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
		if entry.HealthCheck() == nil {
			continue
		}
		if !first {
			str += ","
		}
		str += "(?,?,?)"
		first = false
		args = append(args, entry.ID(), entry.HealthCheck().GetType(),
			entry.HealthCheck().GetHeartbeat().GetTtl().GetValue())
	}
	// 先删除health_check相关数据
	if len(ids) > 0 {
		deleteStr := `delete from health_check where id in (` + builder.String() + `)`
		if _, err := tx.Exec(deleteStr, ids...); err != nil {
			return err
		}
	}
	// 不存在健康检查信息或双写开关关闭，直接返回
	if first || !doubleWrite.Load() {
		return nil
	}
	if log.DebugEnabled() {
		log.Debug("[Store][database] double write instance check", zap.String("sql", str), zap.Any("args", args))
	}
	_, err := tx.Exec(str, args...)
	return err
}

// batchUpdateInstanceMetaV1IfNecessary 批量变更metadata数据
// @note 升级过程中双写，升级后关闭
func batchUpdateInstanceMetaV1IfNecessary(tx *BaseTx, instances []*model.Instance) error {
	if err := batchDeleteInstanceMeta(tx, instances); err != nil {
		return err
	}
	if !doubleWrite.Load() {
		return nil
	}
	if err := batchAddInstanceMeta(tx, instances); err != nil {
		return err
	}
	return nil
}

// SwitchDoubleWrite 双写开关
func SwitchDoubleWrite(enable bool) {
	doubleWrite.Swap(enable)
}
