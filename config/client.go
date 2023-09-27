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
	"context"
	"encoding/base64"

	apiconfig "github.com/polarismesh/specification/source/go/api/v1/config_manage"
	apimodel "github.com/polarismesh/specification/source/go/api/v1/model"
	"go.uber.org/zap"

	api "github.com/polarismesh/polaris/common/api/v1"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/common/rsa"
	commontime "github.com/polarismesh/polaris/common/time"
	"github.com/polarismesh/polaris/common/utils"
)

type (
	compareFunction func(clientInfo *apiconfig.ClientConfigFileInfo, file *model.ConfigFileRelease) bool
)

// GetConfigFileForClient 从缓存中获取配置文件，如果客户端的版本号大于服务端，则服务端重新加载缓存
func (s *Server) GetConfigFileForClient(ctx context.Context,
	client *apiconfig.ClientConfigFileInfo) *apiconfig.ConfigClientResponse {
	namespace := client.GetNamespace().GetValue()
	group := client.GetGroup().GetValue()
	fileName := client.GetFileName().GetValue()
	clientVersion := client.GetVersion().GetValue()

	if namespace == "" || group == "" || fileName == "" {
		return api.NewConfigClientResponseWithInfo(
			apimodel.Code_BadRequest, "namespace & group & fileName can not be empty")
	}
	// 从缓存中获取配置内容
	release := s.fileCache.GetActiveRelease(namespace, group, fileName)
	if release == nil {
		return api.NewConfigClientResponse(apimodel.Code_NotFoundResource, nil)
	}

	// 客户端版本号大于服务端版本号，服务端不返回变更
	if clientVersion > release.Version {
		return api.NewConfigClientResponse(apimodel.Code_DataNoChange, nil)
	}
	configFile, err := toClientInfo(client, release)
	if err != nil {
		log.Error("[Config][Service] get config file to client info", utils.RequestID(ctx), zap.Error(err))
		return api.NewConfigClientResponseWithInfo(apimodel.Code_ExecuteException, err.Error())
	}
	log.Info("[Config][Client] client get config file success.", utils.RequestID(ctx),
		zap.String("client", utils.ParseClientAddress(ctx)), zap.String("file", fileName),
		zap.Uint64("version", release.Version))
	return api.NewConfigClientResponse(apimodel.Code_ExecuteSuccess, configFile)
}

// CreateConfigFileFromClient 调用config_file接口获取配置文件
func (s *Server) CreateConfigFileFromClient(ctx context.Context,
	client *apiconfig.ConfigFile) *apiconfig.ConfigClientResponse {
	configResponse := s.CreateConfigFile(ctx, client)
	return api.NewConfigClientResponseFromConfigResponse(configResponse)
}

// UpdateConfigFileFromClient 调用config_file接口更新配置文件
func (s *Server) UpdateConfigFileFromClient(ctx context.Context,
	client *apiconfig.ConfigFile) *apiconfig.ConfigClientResponse {
	configResponse := s.UpdateConfigFile(ctx, client)
	return api.NewConfigClientResponseFromConfigResponse(configResponse)
}

// PublishConfigFileFromClient 调用config_file_release接口发布配置文件
func (s *Server) PublishConfigFileFromClient(ctx context.Context,
	client *apiconfig.ConfigFileRelease) *apiconfig.ConfigClientResponse {
	configResponse := s.PublishConfigFile(ctx, client)
	return api.NewConfigClientResponseFromConfigResponse(configResponse)
}

func (s *Server) WatchConfigFiles(ctx context.Context,
	req *apiconfig.ClientWatchConfigFileRequest) (WatchCallback, error) {

	watchFiles := req.GetWatchFiles()
	// 2. 检查客户端是否有版本落后
	resp, needWatch := s.checkClientConfigFile(ctx, watchFiles, compareByVersion)
	if !needWatch {
		return func() *apiconfig.ConfigClientResponse {
			return resp
		}, nil
	}

	// 3. 监听配置变更，hold 请求 30s，30s 内如果有配置发布，则响应请求
	clientId := utils.ParseClientAddress(ctx) + "@" + utils.NewUUID()[0:8]
	finishChan := s.WatchCenter().AddWatcher(clientId, watchFiles)
	return func() *apiconfig.ConfigClientResponse {
		return <-finishChan
	}, nil
}

// GetConfigFileNamesWithCache
func (s *Server) GetConfigFileNamesWithCache(ctx context.Context,
	req *apiconfig.ConfigFileGroupRequest) *apiconfig.ConfigClientListResponse {

	namespace := req.GetConfigFileGroup().GetNamespace().GetValue()
	group := req.GetConfigFileGroup().GetName().GetValue()

	out := api.NewConfigClientListResponse(apimodel.Code_ExecuteSuccess)

	if namespace == "" || group == "" {
		out.Code = utils.NewUInt32Value(uint32(apimodel.Code_BadRequest))
		out.Info = utils.NewStringValue("invalid namespace or group")
		return out
	}

	releases, revision := s.fileCache.GetGroupActiveReleases(namespace, group)
	if revision == req.GetRevision().GetValue() {
		out.Code = utils.NewUInt32Value(uint32(apimodel.Code_DataNoChange))
		return out
	}
	ret := make([]*apiconfig.ClientConfigFileInfo, 0, len(releases))
	for i := range releases {
		ret = append(ret, &apiconfig.ClientConfigFileInfo{
			Namespace:   utils.NewStringValue(releases[i].Namespace),
			Group:       utils.NewStringValue(releases[i].Group),
			FileName:    utils.NewStringValue(releases[i].FileName),
			Name:        utils.NewStringValue(releases[i].Name),
			Version:     utils.NewUInt64Value(releases[i].Version),
			ReleaseTime: utils.NewStringValue(commontime.Time2String(releases[i].ModifyTime)),
		})
	}

	return &apiconfig.ConfigClientListResponse{
		Code:            utils.NewUInt32Value(uint32(apimodel.Code_ExecuteSuccess)),
		Info:            utils.NewStringValue(api.Code2Info(uint32(apimodel.Code_ExecuteSuccess))),
		Revision:        utils.NewStringValue(revision),
		Namespace:       namespace,
		Group:           group,
		ConfigFileInfos: ret,
	}
}

func compareByVersion(clientInfo *apiconfig.ClientConfigFileInfo, file *model.ConfigFileRelease) bool {
	return clientInfo.GetVersion().GetValue() < file.Version
}

func compareByMD5(clientInfo *apiconfig.ClientConfigFileInfo, file *model.ConfigFileRelease) bool {
	return clientInfo.Md5.GetValue() != file.Md5
}

func (s *Server) checkClientConfigFile(ctx context.Context, files []*apiconfig.ClientConfigFileInfo,
	compartor compareFunction) (*apiconfig.ConfigClientResponse, bool) {
	if len(files) == 0 {
		return api.NewConfigClientResponse(apimodel.Code_InvalidWatchConfigFileFormat, nil), false
	}
	for _, configFile := range files {
		namespace := configFile.GetNamespace().GetValue()
		group := configFile.GetGroup().GetValue()
		fileName := configFile.GetFileName().GetValue()

		if namespace == "" || group == "" || fileName == "" {
			return api.NewConfigClientResponseWithInfo(apimodel.Code_BadRequest,
				"namespace & group & fileName can not be empty"), false
		}
		// 从缓存中获取最新的配置文件信息
		release := s.fileCache.GetActiveRelease(namespace, group, fileName)
		if release != nil && compartor(configFile, release) {
			ret := &apiconfig.ClientConfigFileInfo{
				Namespace: utils.NewStringValue(namespace),
				Group:     utils.NewStringValue(group),
				FileName:  utils.NewStringValue(fileName),
				Version:   utils.NewUInt64Value(release.Version),
				Md5:       utils.NewStringValue(release.Md5),
			}
			return api.NewConfigClientResponse(apimodel.Code_ExecuteSuccess, ret), false
		}
	}
	return api.NewConfigClientResponse(apimodel.Code_DataNoChange, nil), true
}

func toClientInfo(client *apiconfig.ClientConfigFileInfo,
	release *model.ConfigFileRelease) (*apiconfig.ClientConfigFileInfo, error) {

	namespace := client.GetNamespace().GetValue()
	group := client.GetGroup().GetValue()
	fileName := client.GetFileName().GetValue()
	publicKey := client.GetPublicKey().GetValue()

	copyMetadata := func() map[string]string {
		ret := map[string]string{}
		for k, v := range release.Metadata {
			ret[k] = v
		}
		delete(ret, utils.ConfigFileTagKeyDataKey)
		return ret
	}()

	configFile := &apiconfig.ClientConfigFileInfo{
		Namespace: utils.NewStringValue(namespace),
		Group:     utils.NewStringValue(group),
		FileName:  utils.NewStringValue(fileName),
		Content:   utils.NewStringValue(release.Content),
		Version:   utils.NewUInt64Value(release.Version),
		Md5:       utils.NewStringValue(release.Md5),
		Encrypted: utils.NewBoolValue(release.IsEncrypted()),
		Tags:      model.FromTagMap(copyMetadata),
	}

	dataKey := release.GetEncryptDataKey()
	encryptAlgo := release.GetEncryptAlgo()
	if dataKey != "" && encryptAlgo != "" {
		dataKeyBytes, err := base64.StdEncoding.DecodeString(dataKey)
		if err != nil {
			log.Error("[Config][Service] decode data key error.", zap.String("dataKey", dataKey), zap.Error(err))
			return nil, err
		}
		if publicKey != "" {
			cipherDataKey, err := rsa.EncryptToBase64(dataKeyBytes, publicKey)
			if err != nil {
				log.Error("[Config][Service] rsa encrypt data key error.",
					zap.String("dataKey", dataKey), zap.Error(err))
			} else {
				dataKey = cipherDataKey
			}
		}
		configFile.Tags = append(configFile.Tags,
			&apiconfig.ConfigFileTag{
				Key:   utils.NewStringValue(utils.ConfigFileTagKeyDataKey),
				Value: utils.NewStringValue(dataKey),
			},
		)
	}
	return configFile, nil
}
