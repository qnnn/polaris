package sqldb

import (
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/store"
	"time"
)

type instanceStoreV2 struct {
	master *BaseDB // 大部分操作都用主数据库
	slave  *BaseDB // 缓存相关的读取，请求到slave
}

func (ins *instanceStoreV2) AddInstance(instance *model.Instance) error {
	err := RetryTransaction("addInstance", func() error {
		return ins.addInstance(instance)
	})
	return store.Error(err)
}

func (ins *instanceStoreV2) addInstance(instance *model.Instance) error {
	tx, err := ins.master.Begin()
	if err != nil {
		log.Errorf("[Store][database] add instance tx begin err: %s", err.Error())
		return err
	}
	defer func() { _ = tx.Rollback() }()

	// todo

	return nil
}

func (ins *instanceStoreV2) BatchAddInstances(instances []*model.Instance) error {
	err := RetryTransaction("batchAddInstances", func() error {
		return ins.batchAddInstances(instances)
	})
	return store.Error(err)
}

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
	if err := tx.Commit(); err != nil {
		log.Errorf("[Store][database] batch add instance commit tx err: %s", err.Error())
		return err
	}
	return nil
}

func batchAddMainInstancesV2(tx *BaseTx, instances []*model.Instance) error {
	str := `replace into instance_v2(id, service_id, vpc_id, host, port, protocol, version, health_status, isolate,
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
		metadata, err := json.Marshal(entry.Metadata())
		if err != nil {
			//	todo
		}
		args = append(args, "json", string(metadata))
	}
	_, err := tx.Exec(str, args...)
	return err
}

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
			if err := cleanInstanceV2(tx, instanceID); err != nil {
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
			//str := "update instance set flag = 1, mtime = sysdate() where `id` = ?"
			//if _, err := tx.Exec(str, instanceID); err != nil {
			//	return store.Error(err)
			//}
			//
			//if err := tx.Commit(); err != nil {
			//	log.Errorf("[Store][database] delete instance commit tx err: %s", err.Error())
			//	return err
			//}

			return nil
		})
	})
}

// BatchDeleteInstances 批量删除实例
func (ins *instanceStoreV2) BatchDeleteInstances(ids []interface{}) error {
	return RetryTransaction("batchDeleteInstance", func() error {
		return ins.master.processWithTransaction("batchDeleteInstance", func(tx *BaseTx) error {
			//if err := BatchOperation("delete-instance", ids, func(objects []interface{}) error {
			//	if len(objects) == 0 {
			//		return nil
			//	}
			//	str := `update instance set flag = 1, mtime = sysdate() where id in ( ` + PlaceholdersN(len(objects)) + `)`
			//	_, err := tx.Exec(str, objects...)
			//	return store.Error(err)
			//}); err != nil {
			//	return err
			//}
			//
			//if err := tx.Commit(); err != nil {
			//	log.Errorf("[Store][database] batch delete instance commit tx err: %s", err.Error())
			//	return err
			//}

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

// getInstance 内部获取instance函数，根据instanceID，直接读取元数据，不做其他过滤
func (ins *instanceStoreV2) getInstance(instanceID string) (*model.Instance, error) {
	str := genInstanceSelectSQLV2() + " where instance.id = ?"
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

	return out[0], nil
}

func (ins *instanceStoreV2) BatchGetInstanceIsolate(ids map[string]bool) (map[string]bool, error) {
	return nil, nil
}

func (ins *instanceStoreV2) GetInstancesBrief(ids map[string]bool) (map[string]*model.Instance, error) {
	return nil, nil
}

func (ins *instanceStoreV2) GetInstancesCount() (uint32, error) {
	return 0, nil
}

// GetInstancesCountTx .
func (ins *instanceStoreV2) GetInstancesCountTx(tx store.Tx) (uint32, error) {
	return 0, nil
}

func (ins *instanceStoreV2) GetInstancesMainByService(serviceID, host string) ([]*model.Instance, error) {
	return []*model.Instance{}, nil
}

// GetExpandInstances 根据过滤条件查看对应服务实例及数目
func (ins *instanceStoreV2) GetExpandInstances(filter, metaFilter map[string]string, offset uint32,
	limit uint32) (uint32, []*model.Instance, error) {
	return 0, []*model.Instance{}, nil
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

// getMoreInstancesMainWithMeta 获取增量instance+healthcheck+meta内容
// @note ro库有多个实例，且主库到ro库各实例的同步时间不一致。为避免获取不到meta，需要采用一条sql语句获取全部数据
func (ins *instanceStoreV2) getMoreInstancesMainWithMeta(tx *BaseTx, mtime time.Time, firstUpdate bool, serviceID []string) (
	map[string]*model.Instance, error) {

	str := genInstanceSelectSQLV2() + " where mtime >= FROM_UNIXTIME(?)"
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

// GetInstanceMeta 根据实例ID获取实例的metadata
func (ins *instanceStoreV2) GetInstanceMeta(instanceID string) (map[string]string, error) {
	return map[string]string{}, nil
}

// SetInstanceHealthStatus 设置实例健康状态
func (ins *instanceStoreV2) SetInstanceHealthStatus(instanceID string, flag int, revision string) error {
	return nil
}

// BatchSetInstanceHealthStatus 批量设置健康状态
func (ins *instanceStoreV2) BatchSetInstanceHealthStatus(ids []interface{}, isolate int, revision string) error {
	return nil
}

// BatchSetInstanceIsolate 批量设置实例隔离状态
func (ins *instanceStoreV2) BatchSetInstanceIsolate(ids []interface{}, isolate int, revision string) error {
	return nil
}

// BatchAppendInstanceMetadata 追加实例 metadata
func (ins *instanceStoreV2) BatchAppendInstanceMetadata(requests []*store.InstanceMetadataRequest) error {
	return nil
}

// BatchRemoveInstanceMetadata 删除实例指定的 metadata
func (ins *instanceStoreV2) BatchRemoveInstanceMetadata(requests []*store.InstanceMetadataRequest) error {
	return nil
}

// genInstanceSelectSQL 生成instance的select sql语句
func genInstanceSelectSQLV2() string {
	str := `select id, service_id, IFNULL(vpc_id,""), host, port, IFNULL(protocol, ""), IFNULL(version, ""),
			 health_status, isolate, weight, enable_health_check, IFNULL(logic_set, ""), IFNULL(cmdb_region, ""), 
			 IFNULL(cmdb_zone, ""), IFNULL(cmdb_idc, ""), priority, revision, flag, IFNULL(health_check_type, -1), 
			 IFNULL(ttl, 0),IFNULL(metadata_type, ""), IFNULL(metadata,""), UNIX_TIMESTAMP(ctime), UNIX_TIMESTAMP(mtime)   
			 from instance_v2`
	//-- 			     left join health_check
	//-- 			 on instance.id = health_check.id `
	return str
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
	//out[0].MallocProto()
	//out[0].Proto.Metadata = meta

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
		item.Meta = make(map[string]string)
		err = json.Unmarshal([]byte(metadata), &item.Meta)
		if err != nil {
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

// cleanInstance 清理数据
func cleanInstanceV2(tx *BaseTx, instanceID string) error {
	//log.Infof("[Store][database] clean instance(%s)", instanceID)
	//cleanIns := "delete from instance where id = ? and flag = 1"
	//if _, err := tx.Exec(cleanIns, instanceID); err != nil {
	//	log.Errorf("[Store][database] clean instance(%s), err: %s", instanceID, err.Error())
	//	return store.Error(err)
	//}
	//cleanMeta := "delete from instance_metadata where id = ?"
	//if _, err := tx.Exec(cleanMeta, instanceID); err != nil {
	//	log.Errorf("[Store][database] clean instance_metadata(%s), err: %s", instanceID, err.Error())
	//	return store.Error(err)
	//}
	//cleanCheck := "delete from health_check where id = ?"
	//if _, err := tx.Exec(cleanCheck, instanceID); err != nil {
	//	log.Errorf("[Store][database] clean health_check(%s), err: %s", instanceID, err.Error())
	//	return store.Error(err)
	//}
	return nil
}
