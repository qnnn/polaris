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

package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	EnvDbAddr = "env_db_addr"
	EnvDbName = "env_db_name"
	EnvDbUser = "env_db_user"
	EnvDbPwd  = "env_db_pwd"

	// JobInterval 处理两个批次的服务实例升级的间隔时间，避免变更过于频繁影响到运行中的polaris_server
	JobInterval = 10 * time.Second
	// BatchSize 一个批次的服务实例数量
	BatchSize = 3000
)

var (
	dbAddr string
	dbName string
	dbUser string
	dbPwd  string
)

func init() {
	flag.StringVar(&dbAddr, "db_addr", "", "Input database address")
	flag.StringVar(&dbName, "db_name", "", "Input database name")
	flag.StringVar(&dbUser, "db_user", "", "Input database user")
	flag.StringVar(&dbPwd, "db_pwd", "", "Input database password")
}

const (
	queryInstanceMainSql = `select id from instance where
                            health_check_ttl is null 
			 				and health_check_type is null 
			 				and Metadata is null `
	queryHealthCheckSql = `select instance.id,  health_check.type, health_check.ttl
							from instance left join health_check 
			 				on instance.id = health_check.id where instance.id in (%s)`
	queryInstanceMetaSql  = `select id, mkey, mvalue from instance_metadata where id in (%s)`
	updateInstanceMainSql = `insert into instance (id, service_id, vpc_id, host, port, protocol, version,
                       		 health_status, isolate,weight, enable_health_check, logic_set, cmdb_region,
                      		 cmdb_zone, cmdb_idc, priority, revision, health_check_type, health_check_ttl,
                             Metadata, ctime, mtime)
		 					 value 
		 					 %s
		 					 on DUPLICATE KEY UPDATE 
		 					 health_check_ttl = VALUES(health_check_ttl),
		 					 health_check_type = VALUES(health_check_type),
		 					 Metadata = VALUES(Metadata)`
)

type NewInstance struct {
	ID *wrapperspb.StringValue
	// .....
	CheckType *wrapperspb.Int64Value
	TTL       *wrapperspb.Int64Value
	Metadata  *wrapperspb.StringValue
}

func checkInstanceFields(db *sql.DB) error {
	rows, err := db.Query(`select health_check_ttl, health_check_type, metadata from instance limit 1`)
	if nil != err {
		return err
	}
	defer rows.Close()
	return err
}

func getOldInstanceIds(db *sql.DB) ([]any, error) {
	rows, err := db.Query(queryInstanceMainSql)
	if nil != err {
		log.Printf("get instance err: %s", err.Error())
		return nil, err
	}
	defer rows.Close()
	ids := make([]any, 0)
	for rows.Next() {
		var instanceId string
		err = rows.Scan(&instanceId)
		if err != nil {
			log.Printf("fetch instance scan err: %s", err.Error())
			break
		}
		ids = append(ids, instanceId)
	}

	return ids, err

}

func fetchHealthCheckRows(db *sql.DB, ids []any) ([]*NewInstance, error) {
	rows, err := db.Query(fmt.Sprintf(queryHealthCheckSql, PlaceholdersN(len(ids))), ids...)
	if nil != err {
		log.Printf("get health check err: %s", err.Error())
		return nil, err
	}
	defer rows.Close()
	newInstances := make([]*NewInstance, 0)
	for rows.Next() {
		var instanceId string
		args := make([]interface{}, 2)
		err = rows.Scan(&instanceId, &args[0], &args[1])
		if err != nil {
			log.Printf("fetch health check scan err: %s", err.Error())
			break
		}
		newInstance := &NewInstance{
			ID: wrapperspb.String(instanceId),
		}
		if args[0] != nil {
			newInstance.CheckType = wrapperspb.Int64(args[0].(int64))
		}
		if args[1] != nil {
			newInstance.TTL = wrapperspb.Int64(args[1].(int64))
		}
		newInstances = append(newInstances, newInstance)
	}
	return newInstances, err
}

func fetchInstanceMeta(db *sql.DB, ids []any, newInstances []*NewInstance) error {
	rows, err := db.Query(fmt.Sprintf(queryInstanceMetaSql, PlaceholdersN(len(ids))), ids...)
	if nil != err {
		log.Printf("get instance Metadata err: %s", err.Error())
		return err
	}
	defer rows.Close()
	metadataMap := make(map[string]map[string]string, len(ids))
	for rows.Next() {
		var instanceId, mKey, mValue string
		err = rows.Scan(&instanceId, &mKey, &mValue)
		if err != nil {
			log.Printf("fetch instance Metadata scan err: %s", err.Error())
			return err
		}
		if metadataMap[instanceId] == nil {
			metadataMap[instanceId] = make(map[string]string)
		}
		metadataMap[instanceId][mKey] = mValue
	}
	for _, newInstance := range newInstances {
		meta := metadataMap[newInstance.ID.GetValue()]
		if len(meta) == 0 {
			continue
		}
		metadata, _ := json.Marshal(meta)
		newInstance.Metadata = wrapperspb.String(string(metadata))
	}
	return nil
}

func updateNewInstance(db *sql.DB, newInstances []*NewInstance) error {
	values := make([]string, len(newInstances))
	for count := 0; count < len(values); count++ {
		values[count] =
			`(? ,'', '', '', 0, '', '', 0, 0, 100, 1, '', '', '' , '', 1, '', ?, ?, ?, sysdate(), sysdate())`
	}
	updateSql := fmt.Sprintf(updateInstanceMainSql, strings.Join(values, ","))
	args := make([]interface{}, 0)
	for _, newInstance := range newInstances {
		args = append(args, newInstance.ID.GetValue())
		if newInstance.CheckType != nil {
			args = append(args, newInstance.CheckType.GetValue())
		} else {
			args = append(args, nil)
		}
		if newInstance.TTL != nil {
			args = append(args, newInstance.TTL.GetValue())
		} else {
			args = append(args, nil)
		}
		if newInstance.Metadata != nil {
			args = append(args, newInstance.Metadata.GetValue())
		} else {
			args = append(args, nil)
		}
	}
	err := processWithTransaction(db, func(tx *sql.Tx) error {
		if _, err := tx.Exec(updateSql, args...); err != nil {
			log.Printf("update new instance err: %s", err.Error())
		}
		if err := tx.Commit(); err != nil {
			log.Printf("commit update new instance err: %s", err.Error())
			return err
		}
		return nil
	})
	return err
}

func main() {
	flag.Parse()
	if len(dbAddr) == 0 {
		dbAddr = os.Getenv(EnvDbAddr)
	}
	if len(dbName) == 0 {
		dbName = os.Getenv(EnvDbName)
	}
	if len(dbUser) == 0 {
		dbUser = os.Getenv(EnvDbUser)
	}
	if len(dbPwd) == 0 {
		dbPwd = os.Getenv(EnvDbPwd)
	}
	if len(dbAddr) == 0 || len(dbName) == 0 || len(dbUser) == 0 || len(dbPwd) == 0 {
		log.Printf("invalid arguments: dbAddr %s, dbName %s, dbUser %s, dbPwd %s", dbAddr, dbName, dbUser, dbPwd)
		os.Exit(1)
	}
	dns := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbUser, dbPwd, dbAddr, dbName)
	log.Printf("start to connection database %s", dns)
	db, err := sql.Open("mysql", dns)
	if err != nil {
		log.Printf("sql open err: %s", err.Error())
		os.Exit(1)
	}
	defer db.Close()
	if err := checkInstanceFields(db); err != nil {
		log.Printf(
			"Before executing the upgrade program, it is necessary to add some fields to the instance table. "+
				"Please refer to the instructions in the ./README.me file. err: %s", err)
		os.Exit(1)
	}

	ids, err := getOldInstanceIds(db)
	if err != nil {
		log.Printf("get old instance ids err: %s", err.Error())
		os.Exit(1)
	}
	ticker := time.NewTicker(JobInterval)
	defer ticker.Stop()
	for i := 0; i < len(ids); i += BatchSize {
		select {
		case <-ticker.C:
			newInstances, err := fetchHealthCheckRows(db, ids[i:i+BatchSize])
			if err != nil {
				log.Printf("get old healtch check err: %s", err.Error())
				os.Exit(1)
			}
			if err = fetchInstanceMeta(db, ids[i:i+BatchSize], newInstances); err != nil {
				log.Printf("get old instance Metadata err: %s", err.Error())
				os.Exit(1)
			}
			if err = updateNewInstance(db, newInstances); err != nil {
				os.Exit(1)
			}
			if i+BatchSize < len(ids) {
				log.Printf("upgrade instance progress(%d/%d)", i+BatchSize, len(ids))
			} else {
				log.Printf("upgrade instance progress(%d/%d)", len(ids), len(ids))
			}
			ticker.Reset(JobInterval)
		}
	}
}

func processWithTransaction(db *sql.DB, handle func(*sql.Tx) error) error {
	tx, err := db.Begin()
	if err != nil {
		log.Printf("[Store][database] begin tx err: %s", err.Error())
		return err
	}

	defer func() {
		_ = tx.Rollback()
	}()
	return handle(tx)
}

// PlaceholdersN 构造多个占位符
func PlaceholdersN(size int) string {
	if size <= 0 {
		return ""
	}
	str := strings.Repeat("?,", size)
	return str[0 : len(str)-1]
}
