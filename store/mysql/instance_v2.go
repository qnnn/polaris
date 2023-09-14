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
	"fmt"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/store"
	"go.uber.org/zap"
	"strings"
	"time"
)

// instanceStore 实现了InstanceStore接口
type instanceStoreV2 struct {
	master *BaseDB // 大部分操作都用主数据库
	slave  *BaseDB // 缓存相关的读取，请求到slave
}

// AddInstance 添加实例
func (ins *instanceStoreV2) AddInstance(instance *model.Instance) error {
	err := RetryTransaction("addInstance", func() error {
		return ins.addInstance(instance)
	})
	return store.Error(err)
}

// addInstance
func (ins *instanceStoreV2) addInstance(instance *model.Instance) error {
	tx, err := ins.master.Begin()
	if err != nil {
		log.Errorf("[Store][database] add instance tx begin err: %s", err.Error())
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := batchAddMainInstancesV2(tx, []*model.Instance{instance}); err != nil {
		log.Errorf("[Store][database] add instance insert err: %s", err.Error())
		return err
	}
	// todo 开关双写
	doubleWrite := true
	if doubleWrite {
		if err := addInstanceCheck(tx, instance); err != nil {
			return err
		}

		if err := updateInstanceMeta(tx, instance); err != nil {
			log.Errorf("[Store][database] add instance meta err: %s", err.Error())
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		log.Errorf("[Store][database] add instance commit tx err: %s", err.Error())
		return err
	}

	return nil
}

// BatchAddInstances 批量增加实例
func (ins *instanceStoreV2) BatchAddInstances(instances []*model.Instance) error {

	err := RetryTransaction("batchAddInstances", func() error {
		return ins.batchAddInstances(instances)
	})
	return store.Error(err)
}

// batchAddInstances batch add instances
func (ins *instanceStoreV2) batchAddInstances(instances []*model.Instance) error {
	tx, err := ins.master.Begin()
	if err != nil {
		log.Errorf("[Store][database] batch add instances begin tx err: %s", err.Error())
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if err := batchAddMainInstancesV2(tx, instances); err != nil {
		log.Errorf("[Store][database] batch add main instances err: %s", err.Error())
		return err
	}
	// todo 开关双写
	doubleWrite := true
	if doubleWrite {
		if err := batchAddInstanceCheck(tx, instances); err != nil {
			log.Errorf("[Store][database] batch add instance check err: %s", err.Error())
			return err
		}
		if err := batchDeleteInstanceMeta(tx, instances); err != nil {
			log.Errorf("[Store][database] batch delete instance metadata err: %s", err.Error())
			return err
		}
		if err := batchAddInstanceMeta(tx, instances); err != nil {
			log.Errorf("[Store][database] batch add instance metadata err: %s", err.Error())
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		log.Errorf("[Store][database] batch add instance commit tx err: %s", err.Error())
		return err
	}

	return nil
}

// UpdateInstance 更新实例
func (ins *instanceStoreV2) UpdateInstance(instance *model.Instance) error {
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
func (ins *instanceStoreV2) updateInstance(instance *model.Instance) error {
	tx, err := ins.master.Begin()
	if err != nil {
		log.Errorf("[Store][database] update instance tx begin err: %s", err.Error())
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// todo
	if err := updateInstanceV2(tx, instance); err != nil {
		log.Errorf("[Store][database] update instance err: %s", err.Error())
		return err
	}
	// todo 开关双写
	doubleWrite := true
	if doubleWrite {
		// 更新health check表
		if err := updateInstanceCheck(tx, instance); err != nil {
			log.Errorf("[Store][database] update instance check err: %s", err.Error())
			return err
		}
		// 更新meta表
		if err := updateInstanceMeta(tx, instance); err != nil {
			log.Errorf("[Store][database] update instance meta err: %s", err.Error())
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		log.Errorf("[Store][database] update instance commit tx err: %s", err.Error())
		return err
	}

	return nil
}

// CleanInstance 清理数据
func (ins *instanceStoreV2) CleanInstance(instanceID string) error {
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

// DeleteInstance 删除一个实例，删除实例实际上是把flag置为1
func (ins *instanceStoreV2) DeleteInstance(instanceID string) error {
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
func (ins *instanceStoreV2) BatchDeleteInstances(ids []interface{}) error {
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
func (ins *instanceStoreV2) GetInstance(instanceID string) (*model.Instance, error) {
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
func (ins *instanceStoreV2) BatchGetInstanceIsolate(ids map[string]bool) (map[string]bool, error) {
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
func (ins *instanceStoreV2) GetInstancesBrief(ids map[string]bool) (map[string]*model.Instance, error) {
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
func (ins *instanceStoreV2) GetInstancesCount() (uint32, error) {
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
func (ins *instanceStoreV2) GetInstancesCountTx(tx store.Tx) (uint32, error) {
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
func (ins *instanceStoreV2) GetInstancesMainByService(serviceID, host string) ([]*model.Instance, error) {
	// 只查询有效的服务实例
	str := genInstanceSelectSQLWithoutMetaV2() + " where service_id = ? and host = ? and flag = 0"
	rows, err := ins.master.Query(str, serviceID, host)
	if err != nil {
		log.Errorf("[Store][database] get instances main query err: %s", err.Error())
		return nil, err
	}

	var out []*model.Instance
	err = callFetchInstanceRows(rows, func(entry *model.InstanceStore) (b bool, e error) {
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
func (ins *instanceStoreV2) GetExpandInstances(filter, metaFilter map[string]string, offset uint32,
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
func (ins *instanceStoreV2) getExpandInstances(filter, metaFilter map[string]string, offset uint32,
	limit uint32) ([]*model.Instance, error) {
	// 这种情况意味着，不需要详细的数据，可以不用query了
	if limit == 0 {
		return make([]*model.Instance, 0), nil
	}
	_, isName := filter["name"]
	_, isNamespace := filter["namespace"]
	_, isHost := filter["host"]
	needForceIndex := isName || isNamespace || isHost

	str := genExpandInstanceSelectSQLV2(needForceIndex)
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
func (ins *instanceStoreV2) getExpandInstancesCount(filter, metaFilter map[string]string) (uint32, error) {
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
func (ins *instanceStoreV2) GetMoreInstances(tx store.Tx, mtime time.Time, firstUpdate, needMeta bool,
	serviceID []string) (map[string]*model.Instance, error) {

	dbTx, _ := tx.GetDelegateTx().(*BaseTx)
	if needMeta {
		instances, err := ins.getMoreInstancesMainWithMeta(dbTx, mtime, firstUpdate, serviceID)
		if err != nil {
			return nil, err
		}
		return instances, nil
	}
	// todo 不需要元数据场景
	return map[string]*model.Instance{}, nil
}

// GetInstanceMeta 根据实例ID获取实例的metadata
func (ins *instanceStoreV2) GetInstanceMeta(instanceID string) (map[string]string, error) {
	instance, err := ins.getInstance(instanceID)
	if err != nil {
		log.Errorf("[Store][database] query instance meta err: %s", err.Error())
		return nil, err
	}
	if instance.Metadata() == nil {
		return map[string]string{}, nil
	}
	return instance.Metadata(), err
}

// SetInstanceHealthStatus 设置实例健康状态
func (ins *instanceStoreV2) SetInstanceHealthStatus(instanceID string, flag int, revision string) error {
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
func (ins *instanceStoreV2) BatchSetInstanceHealthStatus(ids []interface{}, isolate int, revision string) error {
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
func (ins *instanceStoreV2) BatchSetInstanceIsolate(ids []interface{}, isolate int, revision string) error {
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
func (ins *instanceStoreV2) BatchAppendInstanceMetadata(requests []*store.InstanceMetadataRequest) error {
	if len(requests) == 0 {
		return nil
	}
	return ins.master.processWithTransaction("AppendInstanceMetadata", func(tx *BaseTx) error {
		for i := range requests {
			id := requests[i].InstanceID
			revision := requests[i].Revision
			appendMetadata := requests[i].Metadata
			sourceMetadata, err := ins.GetInstanceMeta(id)
			if err != nil {
				log.Errorf("[Store][database] append instance metadata query source metadata err: %s", err.Error())
				return err
			}
			for metaKey, metaVal := range appendMetadata {
				sourceMetadata[metaKey] = metaVal
			}

			metadataJson, err := marshalInstanceMetadata(sourceMetadata)
			if err != nil {
				log.Errorf("[Store][database] append instance metadata update revision err: %s", err.Error())
				return err
			}
			str := "update instance set revision = ?, metadata = ?, mtime = sysdate() where id = ?"
			if _, err := tx.Exec(str, revision, metadataJson, id); err != nil {
				log.Errorf("[Store][database] append instance metadata update revision err: %s", err.Error())
				return err
			}
			// todo 开关双写
			doubleWrite := true
			if doubleWrite {
				str = "replace into instance_metadata(`id`, `mkey`, `mvalue`, `ctime`, `mtime`) values"
				values := make([]string, 0, len(appendMetadata))
				args := make([]interface{}, 0, len(appendMetadata)*3)
				for k, v := range appendMetadata {
					values = append(values, "(?, ?, ?, sysdate(), sysdate())")
					args = append(args, id, k, v)
				}
				str += strings.Join(values, ",")
				if log.DebugEnabled() {
					log.Debug("[Store][database] append instance metadata", zap.String("sql", str), zap.Any("args", args))
				}
				if _, err := tx.Exec(str, args...); err != nil {
					log.Errorf("[Store][database] append instance metadata err: %s", err.Error())
					return err
				}
			}
		}
		return tx.Commit()
	})
}

// BatchRemoveInstanceMetadata 删除实例指定的 metadata
func (ins *instanceStoreV2) BatchRemoveInstanceMetadata(requests []*store.InstanceMetadataRequest) error {
	if len(requests) == 0 {
		return nil
	}
	return ins.master.processWithTransaction("RemoveInstanceMetadata", func(tx *BaseTx) error {
		for i := range requests {
			id := requests[i].InstanceID
			revision := requests[i].Revision
			keys := requests[i].Keys
			sourceMetadata, err := ins.GetInstanceMeta(id)
			if err != nil {
				log.Errorf("[Store][database] query instance metadata err: %s", err.Error())
				return err
			}
			for _, key := range keys {
				delete(sourceMetadata, key)
			}

			metadataJson, err := marshalInstanceMetadata(sourceMetadata)
			if err != nil {
				log.Errorf("[Store][database] remove instance metadata update revision err: %s", err.Error())
				return err
			}

			str := "update instance set revision = ?, metadata = ?, mtime = sysdate() where id = ?"
			if _, err := tx.Exec(str, revision, metadataJson, id); err != nil {
				log.Errorf("[Store][database] remove instance metadata by keys update revision err: %s", err.Error())
				return err
			}

			str = "delete from instance_metadata where id = ? and mkey in (%s)"
			values := make([]string, 0, len(keys))
			args := make([]interface{}, 0, 1+len(keys))
			args = append(args, id)
			for i := range keys {
				key := keys[i]
				values = append(values, "?")
				args = append(args, key)
			}
			str = fmt.Sprintf(str, strings.Join(values, ","))

			if _, err := tx.Exec(str, args...); err != nil {
				log.Errorf("[Store][database] remove instance metadata by keys err: %s", err.Error())
				return err
			}
		}
		return tx.Commit()
	})
}

func batchAddMainInstancesV2(tx *BaseTx, instances []*model.Instance) error {
	str := `replace into instance(id, service_id, vpc_id, host, port, protocol, version, health_status, isolate,
		 weight, enable_health_check, logic_set, cmdb_region, cmdb_zone, cmdb_idc, priority, revision, 
		 health_check_type, ttl, metadata_type, metadata, ctime, mtime) 
		 values`
	first := true
	args := make([]interface{}, 0)
	for _, entry := range instances {
		if !first {
			str += ","
		}
		str += "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, sysdate(), sysdate())"
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
			metadata, err := json.Marshal(entry.Metadata())

			if err != nil {
				return err
			}
			args = append(args, "json", string(metadata))
		}
	}
	_, err := tx.Exec(str, args...)
	return err
}

// getInstance 内部获取instance函数，根据instanceID，直接读取元数据，不做其他过滤
func (ins *instanceStoreV2) getInstance(instanceID string) (*model.Instance, error) {
	str := genInstanceSelectSQLWithMetaV2() + " where instance.id = ?"
	rows, err := ins.master.Query(str, instanceID)
	if err != nil {
		log.Errorf("[Store][database] get instance query err: %s", err.Error())
		return nil, err
	}

	out, err := fetchInstanceRowsV2(rows)
	if err != nil {
		return nil, err
	}

	if len(out) == 0 {
		return nil, err
	}

	selectMetadataSql := "select id, mkey, mvalue from instance_metadata where id = ?"
	args := make([]interface{}, 0, len(out))
	args = append(args, instanceID)

	rows, err = ins.master.Query(selectMetadataSql, args)
	if err != nil {
		log.Errorf("[Store][database] acquire instances meta query err: %s", err.Error())
		return nil, err
	}
	if err := fetchInstanceMetaRowsV2(map[string]*model.Instance{out[0].ID(): out[0]}, rows); err != nil {
		return nil, err
	}

	return out[0], nil
}

// getMoreInstancesMainWithMeta 获取增量instance+healthcheck+meta内容
// @note ro库有多个实例，且主库到ro库各实例的同步时间不一致。为避免获取不到meta，需要采用一条sql语句获取全部数据
func (ins *instanceStoreV2) getMoreInstancesMainWithMeta(tx *BaseTx, mtime time.Time, firstUpdate bool, serviceID []string) (
	map[string]*model.Instance, error) {
	// 获取全量服务实例
	instances, err := ins.getMoreInstancesV2(tx, mtime, firstUpdate, serviceID)
	if err != nil {
		log.Errorf("[Store][database] get more instance main err: %s", err.Error())
		return nil, err
	}
	// 首次拉取
	selectMetadataSql := "select id, mkey, mvalue from instance_metadata"
	// 非首次拉取
	args := make([]interface{}, 0, len(instances))
	if !firstUpdate {
		// 获取全量服务实例元数据
		selectMetadataSql += " where id in (" + PlaceholdersN(len(instances))
		selectMetadataSql += ")"
		for instanceId := range instances {
			args = append(args, instanceId)
		}
	}

	rows, err := tx.Query(selectMetadataSql, args)
	if err != nil {
		log.Errorf("[Store][database] acquire instances meta query err: %s", err.Error())
		return nil, err
	}
	if err := fetchInstanceMetaRowsV2(instances, rows); err != nil {
		return nil, err
	}
	return instances, nil
}

func (ins *instanceStoreV2) getMoreInstancesV2(tx *BaseTx, mtime time.Time, firstUpdate bool, serviceID []string) (map[string]*model.Instance, error) {
	// todo left join healthcheck 表
	str := genInstanceSelectSQLWithMetaV2() + " where mtime >= FROM_UNIXTIME(?)"
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
	err = callFetchInstanceRowsV2(rows, func(entry *model.InstanceStore) (b bool, e error) {
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
func (ins *instanceStoreV2) getRowExpandInstances(rows *sql.Rows) ([]*model.Instance, error) {
	if rows == nil {
		return nil, nil
	}
	defer rows.Close()

	var out []*model.Instance
	var item model.ExpandInstanceStore
	var instance model.InstanceStore
	item.ServiceInstance = &instance
	progress := 0
	for rows.Next() {
		progress++
		if progress%50000 == 0 {
			log.Infof("[Store][database] expand instance fetch rows progress: %d", progress)
		}
		var metadataType, metadata string
		err := rows.Scan(&instance.ID, &instance.ServiceID, &instance.VpcID, &instance.Host, &instance.Port,
			&instance.Protocol, &instance.Version, &instance.HealthStatus, &instance.Isolate,
			&instance.Weight, &instance.EnableHealthCheck, &instance.LogicSet, &instance.Region,
			&instance.Zone, &instance.Campus, &instance.Priority, &instance.Revision, &instance.Flag,
			&instance.CheckType, &instance.TTL, &metadataType, &metadata, &item.ServiceName, &item.Namespace,
			&instance.CreateTime, &instance.ModifyTime)
		if err != nil {
			log.Errorf("[Store][database] fetch instance rows err: %s", err.Error())
			return nil, err
		}
		ins := model.ExpandStore2Instance(&item)
		if len(metadata) != 0 {
			ins.Proto.Metadata, err = unMarshalInstanceMetadata(metadata)
			if err != nil {
				return nil, err
			}
		}
		out = append(out, ins)
	}

	if err := rows.Err(); err != nil {
		log.Errorf("[Store][database] instance rows catch err: %s", err.Error())
		return nil, err
	}

	return out, nil
}

// genInstanceSelectSQL 生成instance的select sql语句
func genInstanceSelectSQLWithMetaV2() string {
	str := `select id, service_id, IFNULL(vpc_id,""), host, port, IFNULL(protocol, ""), IFNULL(version, ""),
			 health_status, isolate, weight, enable_health_check, IFNULL(logic_set, ""), IFNULL(cmdb_region, ""), 
			 IFNULL(cmdb_zone, ""), IFNULL(cmdb_idc, ""), priority, revision, flag, 
			 IFNULL(health_check_type, IFNULL(health_check.type, -1)), IFNULL(ttl, IFNULL(health_check.ttl, 0)),
			 IFNULL(metadata_type, ""), IFNULL(metadata, ""), UNIX_TIMESTAMP(ctime), UNIX_TIMESTAMP(mtime)   
			 from instance 
			 left join health_check on instance.id = health_check.id`
	return str
}

func genInstanceSelectSQLWithoutMetaV2() string {
	str := `select id, service_id, IFNULL(vpc_id,""), host, port, IFNULL(protocol, ""), IFNULL(version, ""),
			 health_status, isolate, weight, enable_health_check, IFNULL(logic_set, ""), IFNULL(cmdb_region, ""), 
			 IFNULL(cmdb_zone, ""), IFNULL(cmdb_idc, ""), priority, revision, flag, 
			 IFNULL(health_check_type, IFNULL(health_check.type, -1)), IFNULL(ttl, IFNULL(health_check.ttl, 0)),
			 UNIX_TIMESTAMP(ctime), UNIX_TIMESTAMP(mtime)   
			 from instance 
			 left join health_check on instance.id = health_check.id`
	return str
}

func marshalInstanceMetadata(v any) (string, error) {
	metadata, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return string(metadata), err
}

func unMarshalInstanceMetadata(meta string) (map[string]string, error) {
	metadata := make(map[string]string)
	err := json.Unmarshal([]byte(meta), &metadata)
	return metadata, err
}

// fetchInstanceRows 获取instance rows的内容
func fetchInstanceRowsV2(rows *sql.Rows) ([]*model.Instance, error) {
	var out []*model.Instance
	err := callFetchInstanceRowsV2(rows, func(entry *model.InstanceStore) (b bool, e error) {
		out = append(out, model.Store2Instance(entry))
		return true, nil
	})
	if err != nil {
		log.Errorf("[Store][database] call fetch instance rows err: %s", err.Error())
		return nil, err
	}

	return out, nil
}

// callFetchInstanceRows 带回调的fetch instance
func callFetchInstanceRowsV2(rows *sql.Rows, callback func(entry *model.InstanceStore) (bool, error)) error {
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
		var metadataType, metadata string
		err := rows.Scan(&item.ID, &item.ServiceID, &item.VpcID, &item.Host, &item.Port, &item.Protocol,
			&item.Version, &item.HealthStatus, &item.Isolate, &item.Weight, &item.EnableHealthCheck,
			&item.LogicSet, &item.Region, &item.Zone, &item.Campus, &item.Priority, &item.Revision,
			&item.Flag, &item.CheckType, &item.TTL, &metadataType, &metadata,
			&item.CreateTime, &item.ModifyTime)
		if err != nil {
			log.Errorf("[Store][database] fetch instance rows err: %s", err.Error())
			return err
		}
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

// updateInstanceMain 更新instance主表
func updateInstanceV2(tx *BaseTx, instance *model.Instance) error {
	str := `update instance set protocol = ?,
	 version = ?, health_status = ?, isolate = ?, weight = ?, enable_health_check = ?, logic_set = ?,
	 cmdb_region = ?, cmdb_zone = ?, cmdb_idc = ?, priority = ?, revision = ?, health_check_type = ?, 
	 ttl = ?, metadata_type = ?, metadata = ?, mtime = sysdate() where id = ?`

	metadata, err := marshalInstanceMetadata(instance.Metadata())
	if err != nil {
		return err
	}

	_, err = tx.Exec(str, instance.Protocol(), instance.Version(), instance.Healthy(), instance.Isolate(),
		instance.Weight(), instance.EnableHealthCheck(), instance.LogicSet(),
		instance.Location().GetRegion().GetValue(), instance.Location().GetZone().GetValue(),
		instance.Location().GetCampus().GetValue(), instance.Priority(), instance.Revision(),
		instance.HealthCheck().GetType(), instance.HealthCheck().GetHeartbeat().GetTtl(),
		"json", metadata, instance.ID())

	return err
}

// fetchInstanceMetaRows 解析获取instance metadata
func fetchInstanceMetaRowsV2(instances map[string]*model.Instance, rows *sql.Rows) error {
	if rows == nil {
		return nil
	}
	defer rows.Close()
	var id, key, value string
	progress := 0
	for rows.Next() {
		progress++
		if progress%500000 == 0 {
			log.Infof("[Store][database] fetch instance meta progress: %d", progress)
		}
		if err := rows.Scan(&id, &key, &value); err != nil {
			log.Errorf("[Store][database] fetch instance metadata rows scan err: %s", err.Error())
			return err
		}
		// 不在目标列表，不存储
		if _, ok := instances[id]; !ok {
			continue
		}
		// 已存在新instance字段中，不存储
		if instances[id].Proto.Metadata != nil {
			continue
		}
		if instances[id].Proto.Metadata == nil {
			instances[id].Proto.Metadata = make(map[string]string)
		}
		instances[id].Proto.Metadata[key] = value
	}
	if err := rows.Err(); err != nil {
		log.Errorf("[Store][database] fetch instance metadata rows next err: %s", err.Error())
		return err
	}

	return nil
}

// genExpandInstanceSelectSQL 生成expandInstance的select sql语句
func genExpandInstanceSelectSQLV2(needForceIndex bool) string {
	str := `select instance.id, service_id, IFNULL(vpc_id,""), host, port, IFNULL(protocol, ""), IFNULL(version, ""),
					 health_status, isolate, weight, enable_health_check, IFNULL(logic_set, ""), IFNULL(cmdb_region, ""), 
					 IFNULL(cmdb_zone, ""), IFNULL(cmdb_idc, ""), priority, instance.revision, instance.flag, 
					 IFNULL(health_check_type, IFNULL(health_check.type, -1)), IFNULL(ttl, IFNULL(health_check.ttl, 0)),
					 IFNULL(metadata_type, ""), IFNULL(metadata, ""), service.name, service.namespace, 
					 UNIX_TIMESTAMP(instance.ctime), UNIX_TIMESTAMP(instance.mtime) 
					 from (service inner join instance `
	if needForceIndex {
		str += `force index(service_id, host) `
	}
	str += `on service.id = instance.service_id) left join health_check on instance.id = health_check.id `
	return str
}
