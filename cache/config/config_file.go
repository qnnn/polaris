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

package config

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.etcd.io/bbolt"
	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	types "github.com/polarismesh/polaris/cache/api"
	"github.com/polarismesh/polaris/common/eventhub"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/common/utils"
	"github.com/polarismesh/polaris/store"
)

type fileCache struct {
	*types.BaseCache
	storage store.Store
	// releases config_release.id -> model.SimpleConfigFileRelease
	releases *utils.SegmentMap[uint64, *model.SimpleConfigFileRelease]
	// name2release namespace -> group -> file_name -> []model.ConfigFileRelease
	name2release *utils.SyncMap[string, *utils.SyncMap[string, *utils.SyncMap[string,
		*utils.SyncMap[string, *model.SimpleConfigFileRelease]]]]
	// activeReleases namespace -> group -> []model.ConfigFileRelease
	activeReleases *utils.SyncMap[string, *utils.SyncMap[string, *utils.SyncMap[string, *model.SimpleConfigFileRelease]]]
	// groupedActiveReleaseRevisions namespace -> group -> revision
	activeReleaseRevisions *utils.SyncMap[string, *utils.SyncMap[string, string]]
	// singleGroup
	singleGroup *singleflight.Group
	// valueCache save ConfigFileRelease.Content into local file to reduce memory use
	valueCache *bbolt.DB
	// metricsReleaseCount
	metricsReleaseCount *utils.SyncMap[string, *utils.SyncMap[string, uint64]]
	// preMetricsFiles
	preMetricsFiles *utils.AtomicValue[map[string]map[string]struct{}]
	// lastReportTime
	lastReportTime *utils.AtomicValue[time.Time]
}

// NewConfigFileCache 创建文件缓存
func NewConfigFileCache(storage store.Store, cacheMgr types.CacheManager) types.ConfigFileCache {
	cache := &fileCache{
		BaseCache: types.NewBaseCache(storage, cacheMgr),
		storage:   storage,
	}
	return cache
}

// Initialize
func (fc *fileCache) Initialize(opt map[string]interface{}) error {
	fc.releases = utils.NewSegmentMap[uint64, *model.SimpleConfigFileRelease](1, func(k uint64) int {
		return int(k)
	})
	fc.name2release = utils.NewSyncMap[string, *utils.SyncMap[string, *utils.SyncMap[string,
		*utils.SyncMap[string, *model.SimpleConfigFileRelease]]]]()
	fc.activeReleases = utils.NewSyncMap[string, *utils.SyncMap[string,
		*utils.SyncMap[string, *model.SimpleConfigFileRelease]]]()
	fc.activeReleaseRevisions = utils.NewSyncMap[string, *utils.SyncMap[string, string]]()
	fc.singleGroup = &singleflight.Group{}
	fc.metricsReleaseCount = utils.NewSyncMap[string, *utils.SyncMap[string, uint64]]()
	fc.preMetricsFiles = utils.NewAtomicValue[map[string]map[string]struct{}](map[string]map[string]struct{}{})
	fc.lastReportTime = utils.NewAtomicValue[time.Time](time.Time{})
	valueCache, err := openBoltCache(opt)
	if err != nil {
		return err
	}
	fc.valueCache = valueCache
	return nil
}

func openBoltCache(opt map[string]interface{}) (*bbolt.DB, error) {
	path, _ := opt["cachePath"].(string)
	if path == "" {
		path = "./data/cache/config"
	}
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}
	dbFile := filepath.Join(path, "config_file.bolt")
	_ = os.Remove(dbFile)
	valueCache, err := bbolt.Open(dbFile, os.ModePerm, &bbolt.Options{})
	if err != nil {
		return nil, err
	}
	return valueCache, nil
}

// Update 更新缓存函数
func (fc *fileCache) Update() error {
	err, _ := fc.singleUpdate()
	return err
}

func (fc *fileCache) singleUpdate() (error, bool) {
	// 多个线程竞争，只有一个线程进行更新
	_, err, shared := fc.singleGroup.Do(fc.Name(), func() (interface{}, error) {
		defer func() {
			fc.reportMetricsInfo()
		}()
		return nil, fc.DoCacheUpdate(fc.Name(), fc.realUpdate)
	})
	return err, shared
}

func (fc *fileCache) realUpdate() (map[string]time.Time, int64, error) {
	// 拉取diff前的所有数据
	start := time.Now()
	releases, err := fc.storage.GetMoreReleaseFile(fc.IsFirstUpdate(), fc.LastFetchTime())
	if err != nil {
		return nil, 0, err
	}
	if len(releases) == 0 {
		return nil, 0, nil
	}

	lastMimes, update, del, err := fc.setReleases(releases)
	if err != nil {
		return nil, 0, err
	}
	log.Info("[Cache][ConfigReleases] get more releases",
		zap.Int("update", update), zap.Int("delete", del),
		zap.Time("last", fc.LastMtime()), zap.Duration("used", time.Since(start)))
	return lastMimes, int64(len(releases)), err
}

func (fc *fileCache) setReleases(releases []*model.ConfigFileRelease) (map[string]time.Time, int, int, error) {
	lastMtime := fc.LastMtime().Unix()
	update := 0
	del := 0

	affect := map[string]map[string]struct{}{}

	for i := range releases {
		item := releases[i]
		if _, ok := affect[item.Namespace]; !ok {
			affect[item.Namespace] = map[string]struct{}{}
		}
		affect[item.Namespace][item.Group] = struct{}{}

		modifyUnix := item.ModifyTime.Unix()
		if modifyUnix > lastMtime {
			lastMtime = modifyUnix
		}
		oldVal, _ := fc.releases.Get(item.Id)
		if !item.Valid {
			del++
			if err := fc.handleDeleteRelease(oldVal, item); err != nil {
				return nil, update, del, err
			}
		} else {
			update++
			if err := fc.handleUpdateRelease(oldVal, item); err != nil {
				return nil, update, del, err
			}
		}

		if item.Active {
			configLog.Info("[Config][Release][Cache] notify config release change",
				zap.Any("info", item.SimpleConfigFileRelease))
			fc.sendEvent(item)
		}
	}
	fc.postProcessUpdatedRelease(affect)
	return map[string]time.Time{fc.Name(): time.Unix(lastMtime, 0)}, update, del, nil
}

func (fc *fileCache) sendEvent(item *model.ConfigFileRelease) {
	err := eventhub.Publish(eventhub.ConfigFilePublishTopic, &eventhub.PublishConfigFileEvent{
		Message: item.SimpleConfigFileRelease,
	})
	if err != nil {
		configLog.Error("[Config][Release][Cache] notify config release change",
			zap.Any("info", item.ConfigFileReleaseKey), zap.Error(err))
	}
}

// handleUpdateRelease
func (fc *fileCache) handleUpdateRelease(oldVal *model.SimpleConfigFileRelease, item *model.ConfigFileRelease) error {
	fc.releases.Put(item.Id, item.SimpleConfigFileRelease)

	func() {
		// 记录 namespace -> group -> file_name -> []SimpleRelease 信息
		if _, ok := fc.name2release.Load(item.Namespace); !ok {
			fc.name2release.Store(item.Namespace, utils.NewSyncMap[string,
				*utils.SyncMap[string, *utils.SyncMap[string, *model.SimpleConfigFileRelease]]]())
		}
		namespace, _ := fc.name2release.Load(item.Namespace)
		if _, ok := namespace.Load(item.Group); !ok {
			namespace.Store(item.Group, utils.NewSyncMap[string, *utils.SyncMap[string, *model.SimpleConfigFileRelease]]())
		}
		group, _ := namespace.Load(item.Group)
		group.ComputeIfAbsent(item.FileName, func(k string) *utils.SyncMap[string, *model.SimpleConfigFileRelease] {
			return utils.NewSyncMap[string, *model.SimpleConfigFileRelease]()
		})
		files, _ := group.Load(item.FileName)
		files.Store(item.Name, item.SimpleConfigFileRelease)
	}()

	if !item.Active {
		return nil
	}

	func() {
		// 保存 active 状态的所有发布 release 信息
		if _, ok := fc.activeReleases.Load(item.Namespace); !ok {
			fc.activeReleases.Store(item.Namespace, utils.NewSyncMap[string,
				*utils.SyncMap[string, *model.SimpleConfigFileRelease]]())
		}
		namespace, _ := fc.activeReleases.Load(item.Namespace)
		if _, ok := namespace.Load(item.Group); !ok {
			namespace.Store(item.Group, utils.NewSyncMap[string, *model.SimpleConfigFileRelease]())
		}
		group, _ := namespace.Load(item.Group)
		group.Store(item.ActiveKey(), item.SimpleConfigFileRelease)
	}()

	if err := fc.valueCache.Update(func(tx *bbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(item.OwnerKey()))
		if err != nil {
			return err
		}
		return bucket.Put([]byte(item.ActiveKey()), []byte(item.Content))
	}); err != nil {
		return errors.Join(err, errors.New("persistent config_file content fail"))
	}
	return nil
}

// handleDeleteRelease
func (fc *fileCache) handleDeleteRelease(oldVal *model.SimpleConfigFileRelease, item *model.ConfigFileRelease) error {
	fc.releases.Del(item.Id)

	func() {
		// 记录 namespace -> group -> file_name -> []SimpleRelease 信息
		if _, ok := fc.name2release.Load(item.Namespace); !ok {
			return
		}
		namespace, _ := fc.name2release.Load(item.Namespace)
		if _, ok := namespace.Load(item.Group); !ok {
			return
		}
		group, _ := namespace.Load(item.Group)
		if _, ok := group.Load(item.FileName); !ok {
			return
		}

		files, _ := group.Load(item.FileName)
		files.Delete(item.Name)

		if files.Len() == 0 {
			group.Delete(item.FileName)
		}
	}()

	if oldVal == nil {
		return nil
	}
	if !oldVal.Active {
		return nil
	}
	if namespace, ok := fc.activeReleases.Load(item.Namespace); ok {
		if group, ok := namespace.Load(item.Group); ok {
			group.Delete(item.ActiveKey())
		}
	}
	if err := fc.valueCache.Update(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(item.OwnerKey()))
		if bucket == nil {
			return nil
		}
		return bucket.Delete([]byte(item.ActiveKey()))
	}); err != nil {
		return errors.Join(err, errors.New("remove config_file content fail"))
	}
	return nil
}

// postProcessUpdatedRelease
func (fc *fileCache) postProcessUpdatedRelease(affect map[string]map[string]struct{}) {
	for ns, groups := range affect {
		nsBucket, ok := fc.name2release.Load(ns)
		if !ok {
			continue
		}
		if _, ok := fc.metricsReleaseCount.Load(ns); !ok {
			fc.metricsReleaseCount.Store(ns, utils.NewSyncMap[string, uint64]())
		}
		nsMetric, _ := fc.metricsReleaseCount.Load(ns)
		for group := range groups {
			groupBucket, ok := nsBucket.Load(group)
			if !ok {
				continue
			}
			nsMetric.Store(group, uint64(groupBucket.Len()))
		}
	}
}

func (fc *fileCache) LastMtime() time.Time {
	return fc.BaseCache.LastMtime(fc.Name())
}

// Clear
func (fc *fileCache) Clear() error {
	return nil
}

func (fc *fileCache) Close() error {
	if fc.valueCache != nil {
		if err := fc.valueCache.Close(); err != nil {
			return err
		}
	}
	return nil
}

// name
func (fc *fileCache) Name() string {
	return types.ConfigFileCacheName
}

// GetActiveRelease
func (fc *fileCache) GetGroupActiveReleases(namespace, group string) ([]*model.ConfigFileRelease, string) {
	nsBucket, ok := fc.activeReleases.Load(namespace)
	if !ok {
		return nil, ""
	}
	groupBucket, ok := nsBucket.Load(group)
	if !ok {
		return nil, ""
	}
	ret := make([]*model.ConfigFileRelease, 0, 8)
	groupBucket.Range(func(key string, val *model.SimpleConfigFileRelease) bool {
		ret = append(ret, &model.ConfigFileRelease{
			SimpleConfigFileRelease: val,
		})
		return true
	})
	groupRevisions, ok := fc.activeReleaseRevisions.Load(namespace)
	if !ok {
		return ret, utils.NewUUID()
	}
	revision, _ := groupRevisions.Load(group)
	return ret, revision
}

// GetActiveRelease
func (fc *fileCache) GetActiveRelease(namespace, group, fileName string) *model.ConfigFileRelease {
	nsBucket, ok := fc.activeReleases.Load(namespace)
	if !ok {
		return nil
	}
	groupBucket, ok := nsBucket.Load(group)
	if !ok {
		return nil
	}
	searchKey := &model.ConfigFileReleaseKey{
		Namespace: namespace,
		Group:     group,
		FileName:  fileName,
	}
	simple, ok := groupBucket.Load(searchKey.ActiveKey())
	if !ok {
		return nil
	}
	ret := &model.ConfigFileRelease{
		SimpleConfigFileRelease: simple,
	}
	fc.loadValueCache(ret)
	return ret
}

// GetRelease
func (fc *fileCache) GetRelease(key model.ConfigFileReleaseKey) *model.ConfigFileRelease {
	var (
		simple *model.SimpleConfigFileRelease
	)
	if key.Id != 0 {
		simple, _ = fc.releases.Get(key.Id)
	} else {
		nsB, ok := fc.name2release.Load(key.Namespace)
		if !ok {
			return nil
		}
		groupB, ok := nsB.Load(key.Group)
		if !ok {
			return nil
		}
		fileB, ok := groupB.Load(key.FileName)
		if !ok {
			return nil
		}
		simple, _ = fileB.Load(key.Name)
	}
	if simple == nil {
		return nil
	}
	ret := &model.ConfigFileRelease{
		SimpleConfigFileRelease: simple,
	}
	fc.loadValueCache(ret)
	return ret
}

func (fc *fileCache) QueryReleases(args *types.ConfigReleaseArgs) (uint32, []*model.SimpleConfigFileRelease, error) {
	if err := fc.Update(); err != nil {
		return 0, nil, err
	}

	values := make([]*model.SimpleConfigFileRelease, 0, args.Limit)
	fc.name2release.Range(func(namespace string, groups *utils.SyncMap[string, *utils.SyncMap[string,
		*utils.SyncMap[string, *model.SimpleConfigFileRelease]]]) bool {

		if args.Namespace != "" && utils.IsWildNotMatch(namespace, args.Namespace) {
			return true
		}
		groups.Range(func(group string, files *utils.SyncMap[string, *utils.SyncMap[string,
			*model.SimpleConfigFileRelease]]) bool {

			if args.Group != "" && utils.IsWildNotMatch(group, args.Group) {
				return true
			}
			files.Range(func(fileName string, releases *utils.SyncMap[string, *model.SimpleConfigFileRelease]) bool {
				if args.FileName != "" && utils.IsWildNotMatch(fileName, args.FileName) {
					return true
				}
				releases.Range(func(releaseName string, item *model.SimpleConfigFileRelease) bool {
					if args.ReleaseName != "" && utils.IsWildNotMatch(item.Name, args.ReleaseName) {
						return true
					}
					if args.OnlyActive && !item.Active {
						return true
					}
					if len(args.Metadata) > 0 {
						for k, v := range args.Metadata {
							sv := item.Metadata[k]
							if sv != v {
								return true
							}
						}
					}

					values = append(values, item)
					return true
				})
				return true
			})
			return true
		})
		return true
	})

	sort.Slice(values, func(i, j int) bool {
		asc := strings.ToLower(args.OrderType) == "asc"
		if strings.ToLower(args.OrderField) == "name" {
			return orderByConfigReleaseName(values[i], values[j], asc)
		}
		if strings.ToLower(args.OrderField) == "mtime" {
			return orderByConfigReleaseMtime(values[i], values[j], asc)
		}
		return orderByConfigReleaseVersion(values[i], values[j], asc)
	})

	return uint32(len(values)), doPageConfigReleases(values, args), nil
}

func orderByConfigReleaseName(a, b *model.SimpleConfigFileRelease, asc bool) bool {
	if asc {
		return a.Name <= b.Name
	}
	return a.Name > b.Name
}

func orderByConfigReleaseMtime(a, b *model.SimpleConfigFileRelease, asc bool) bool {
	if asc {
		return a.ModifyTime.Before(b.ModifyTime)
	}
	return a.ModifyTime.After(b.ModifyTime)
}

func orderByConfigReleaseVersion(a, b *model.SimpleConfigFileRelease, asc bool) bool {
	if asc {
		return a.Version < b.Version
	}
	return a.Version > b.Version
}

func doPageConfigReleases(values []*model.SimpleConfigFileRelease,
	args *types.ConfigReleaseArgs) []*model.SimpleConfigFileRelease {

	if args.NoPage {
		return values
	}

	offset := args.Offset
	limit := args.Limit

	amount := uint32(len(values))
	if offset >= amount || limit == 0 {
		return nil
	}
	endIdx := offset + limit
	if endIdx > amount {
		endIdx = amount
	}
	return values[offset:endIdx]
}

func (fc *fileCache) loadValueCache(release *model.ConfigFileRelease) {
	_ = fc.valueCache.View(func(tx *bbolt.Tx) error {
		bucket := tx.Bucket([]byte(release.OwnerKey()))
		if bucket == nil {
			return nil
		}
		val := bucket.Get([]byte(release.ActiveKey()))
		release.Content = string(val)
		return nil
	})
}
