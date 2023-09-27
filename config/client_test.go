/*
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

package config_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	apiconfig "github.com/polarismesh/specification/source/go/api/v1/config_manage"
	apimodel "github.com/polarismesh/specification/source/go/api/v1/model"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/wrapperspb"

	api "github.com/polarismesh/polaris/common/api/v1"
	commonlog "github.com/polarismesh/polaris/common/log"
	"github.com/polarismesh/polaris/common/model"
	"github.com/polarismesh/polaris/common/utils"
	"github.com/polarismesh/polaris/config"
	testsuit "github.com/polarismesh/polaris/test/suit"
)

// TestClientSetupAndFileNotExisted 测试客户端启动时（version=0），并且配置不存在的情况下拉取配置
func TestClientSetupAndFileNotExisted(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	fileInfo := &apiconfig.ClientConfigFileInfo{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		FileName:  &wrapperspb.StringValue{Value: testFile},
		Version:   &wrapperspb.UInt64Value{Value: 0},
	}

	rsp := testSuit.ConfigServer().GetConfigFileForClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, uint32(api.NotFoundResource), rsp.Code.GetValue(), "GetConfigFileForClient must notfound")

	originSvr := testSuit.OriginConfigServer()
	rsp2, _ := originSvr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(0), config.TestCompareByVersion)
	assert.Equal(t, uint32(api.DataNoChange), rsp2.Code.GetValue(), "checkClientConfigFileByVersion must nochange")
	assert.Nil(t, rsp2.ConfigFile)

	rsp3, _ := originSvr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(0), config.TestCompareByMD5)
	assert.Equal(t, uint32(api.DataNoChange), rsp3.Code.GetValue())
	assert.Nil(t, rsp3.ConfigFile)
}

// TestClientSetupAndFileExisted 测试客户端启动时（version=0），并且配置存在的情况下拉取配置
func TestClientSetupAndFileExisted(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	// 创建并发布一个配置文件
	configFile := assembleConfigFile()
	rsp := testSuit.ConfigServer().CreateConfigFile(testSuit.DefaultCtx, configFile)
	assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue(), rsp.GetInfo().GetValue())

	rsp2 := testSuit.ConfigServer().PublishConfigFile(testSuit.DefaultCtx, assembleConfigFileRelease(configFile))
	assert.Equal(t, api.ExecuteSuccess, rsp2.Code.GetValue(), rsp.GetInfo().GetValue())

	fileInfo := &apiconfig.ClientConfigFileInfo{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		FileName:  &wrapperspb.StringValue{Value: testFile},
		Version:   &wrapperspb.UInt64Value{Value: 0},
	}

	// 强制 Cache 层 load 到本地
	_ = testSuit.DiscoverServer().Cache().ConfigFile().Update()

	// 拉取配置接口
	rsp3 := testSuit.ConfigServer().GetConfigFileForClient(testSuit.DefaultCtx, fileInfo)
	assert.Equalf(t, api.ExecuteSuccess, rsp3.Code.GetValue(), "GetConfigFileForClient must success, acutal code : %d", rsp3.Code.GetValue())
	assert.NotNil(t, rsp3.ConfigFile)
	assert.Equal(t, uint64(1), rsp3.ConfigFile.Version.GetValue())
	assert.Equal(t, configFile.Content.GetValue(), rsp3.ConfigFile.Content.GetValue())
	assert.Equal(t, config.CalMd5(configFile.Content.GetValue()), rsp3.ConfigFile.Md5.GetValue())

	// 比较客户端配置是否落后
	originSvr := testSuit.OriginConfigServer()
	rsp4, _ := originSvr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(0), config.TestCompareByVersion)
	assert.Equal(t, api.ExecuteSuccess, rsp4.Code.GetValue(), rsp4.GetInfo().GetValue())
	assert.NotNil(t, rsp4.ConfigFile)
	assert.Equal(t, config.CalMd5(configFile.Content.GetValue()), rsp4.ConfigFile.Md5.GetValue())

	rsp5, _ := originSvr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(0), config.TestCompareByMD5)
	assert.Equal(t, api.ExecuteSuccess, rsp5.Code.GetValue(), rsp5.GetInfo().GetValue())
	assert.NotNil(t, rsp5.ConfigFile)
	assert.Equal(t, uint64(1), rsp5.ConfigFile.Version.GetValue())
	assert.Equal(t, config.CalMd5(configFile.Content.GetValue()), rsp5.ConfigFile.Md5.GetValue())
}

// TestClientSetupAndCreateNewFile 测试客户端启动时（version=0），并且配置不存在的情况下创建新的配置
func TestClientSetupAndCreateNewFile(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	fileInfo := &apiconfig.ConfigFile{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		Name:      &wrapperspb.StringValue{Value: testFile},
		Content:   &wrapperspb.StringValue{Value: testContent},
	}

	rsp := testSuit.ConfigServer().CreateConfigFileFromClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue(), rsp.GetInfo().GetValue())

	originSvr := testSuit.OriginConfigServer()
	rsp2, _ := originSvr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(0), config.TestCompareByVersion)
	assert.Equal(t, api.DataNoChange, rsp2.Code.GetValue(), rsp2.GetInfo().GetValue())
	assert.Nil(t, rsp2.ConfigFile)

	rsp3, _ := originSvr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(0), config.TestCompareByMD5)
	assert.Equal(t, api.DataNoChange, rsp3.Code.GetValue(), rsp3.GetInfo().GetValue())
	assert.Nil(t, rsp3.ConfigFile)
}

// TestClientSetupAndCreateExistFile 测试客户端启动时（version=0），并且配置存在的情况下重复创建配置
func TestClientSetupAndCreateExistFile(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	fileInfo := &apiconfig.ConfigFile{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		Name:      &wrapperspb.StringValue{Value: testFile},
		Content:   &wrapperspb.StringValue{Value: testContent},
	}

	// 第一次创建
	rsp := testSuit.ConfigServer().CreateConfigFileFromClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue(), "First CreateConfigFileFromClient must success")

	// 第二次创建
	rsp1 := testSuit.ConfigServer().CreateConfigFileFromClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, api.ExistedResource, rsp1.Code.GetValue(), "CreateConfigFileFromClient again must error")

	originSvr := testSuit.OriginConfigServer()
	rsp2, _ := originSvr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(0), config.TestCompareByVersion)
	assert.Equal(t, api.DataNoChange, rsp2.Code.GetValue(), "checkClientConfigFileByVersion must nochange")
	assert.Nil(t, rsp2.ConfigFile)

	rsp3, _ := originSvr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(0), config.TestCompareByMD5)
	assert.Equal(t, api.DataNoChange, rsp3.Code.GetValue())
	assert.Nil(t, rsp3.ConfigFile)
}

// TestClientSetupAndUpdateNewFile 测试客户端启动时（version=0），更新不存在的配置
func TestClientSetupAndUpdateNewFile(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	fileInfo := &apiconfig.ConfigFile{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		Name:      &wrapperspb.StringValue{Value: testFile},
		Content:   &wrapperspb.StringValue{Value: testContent},
	}

	// 直接更新
	rsp := testSuit.ConfigServer().UpdateConfigFileFromClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, api.NotFoundResource, rsp.Code.GetValue(), "UpdateConfigFileFromClient with no exist file must error")
}

// TestClientSetupAndUpdateExistFile 测试客户端启动时（version=0），更新存在的配置
func TestClientSetupAndUpdateExistFile(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	fileInfo := &apiconfig.ConfigFile{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		Name:      &wrapperspb.StringValue{Value: testFile},
		Content:   &wrapperspb.StringValue{Value: testContent},
	}

	// 先创建
	rsp := testSuit.ConfigServer().CreateConfigFileFromClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue(), "CreateConfigFileFromClient must success")

	// 再更新
	fileInfo.Content = &wrapperspb.StringValue{Value: testContent + "1"}
	rsp1 := testSuit.ConfigServer().UpdateConfigFileFromClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, api.ExecuteSuccess, rsp1.Code.GetValue(), "UpdateConfigFileFromClient must success")

}

// TestClientSetupAndPublishNewFile 测试客户端启动时（version=0），发布不存在的配置
func TestClientSetupAndPublishNewFile(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	fileReleaseInfo := &apiconfig.ConfigFileRelease{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		FileName:  &wrapperspb.StringValue{Value: testFile},
		Content:   &wrapperspb.StringValue{Value: testContent},
	}

	// 直接发布
	rsp := testSuit.ConfigServer().PublishConfigFileFromClient(testSuit.DefaultCtx, fileReleaseInfo)
	assert.Equal(t, api.NotFoundNamespace, rsp.Code.GetValue(), "PublishConfigFileFromClient with no exist file must error")
}

// TestClientSetupAndPublishExistFile 测试客户端启动时（version=0），发布存在的配置
func TestClientSetupAndPublishExistFile(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	fileInfo := &apiconfig.ConfigFile{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		Name:      &wrapperspb.StringValue{Value: testFile},
		Content:   &wrapperspb.StringValue{Value: testContent},
	}

	// 先创建
	rsp := testSuit.ConfigServer().CreateConfigFileFromClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue(), "CreateConfigFileFromClient must success")

	// 再发布
	fileReleaseInfo := &apiconfig.ConfigFileRelease{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		FileName:  &wrapperspb.StringValue{Value: testFile},
		Content:   &wrapperspb.StringValue{Value: testContent},
	}
	rsp1 := testSuit.ConfigServer().PublishConfigFileFromClient(testSuit.DefaultCtx, fileReleaseInfo)
	assert.Equal(t, api.ExecuteSuccess, rsp1.Code.GetValue(), "PublishConfigFileFromClient must success")

}

// TestClientVersionBehindServer 测试客户端版本落后服务端
func TestClientVersionBehindServer(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	// 创建并连续发布5次
	configFile := assembleConfigFile()
	rsp := testSuit.ConfigServer().CreateConfigFile(testSuit.DefaultCtx, configFile)
	assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue())

	for i := 0; i < 5; i++ {
		configFile.Content = utils.NewStringValue("content" + strconv.Itoa(i))
		// 更新
		rsp2 := testSuit.ConfigServer().UpdateConfigFile(testSuit.DefaultCtx, configFile)
		assert.Equal(t, api.ExecuteSuccess, rsp2.Code.GetValue())
		// 发布
		rsp3 := testSuit.ConfigServer().PublishConfigFile(testSuit.DefaultCtx, assembleConfigFileRelease(configFile))
		assert.Equal(t, api.ExecuteSuccess, rsp3.Code.GetValue(), rsp3.GetInfo().GetValue())
	}

	// 客户端版本号为4， 服务端由于连续发布5次，所以版本号为5
	clientVersion := uint64(4)
	latestContent := "content4"

	fileInfo := &apiconfig.ClientConfigFileInfo{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		FileName:  &wrapperspb.StringValue{Value: testFile},
		Version:   &wrapperspb.UInt64Value{Value: clientVersion},
	}

	_ = testSuit.DiscoverServer().Cache().ConfigFile().Update()

	// 拉取配置接口
	rsp4 := testSuit.ConfigServer().GetConfigFileForClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, api.ExecuteSuccess, rsp4.Code.GetValue())
	assert.NotNil(t, rsp4.ConfigFile)
	assert.Equal(t, uint64(5), rsp4.ConfigFile.Version.GetValue())
	assert.Equal(t, latestContent, rsp4.ConfigFile.Content.GetValue())
	assert.Equal(t, config.CalMd5(latestContent), rsp4.ConfigFile.Md5.GetValue())

	svr := testSuit.OriginConfigServer()
	// 比较客户端配置是否落后
	rsp5, _ := svr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(clientVersion), config.TestCompareByVersion)
	assert.Equal(t, api.ExecuteSuccess, rsp5.Code.GetValue())
	assert.NotNil(t, rsp5.ConfigFile)
	assert.Equal(t, config.CalMd5(latestContent), rsp5.ConfigFile.Md5.GetValue())

	rsp6, _ := svr.TestCheckClientConfigFile(testSuit.DefaultCtx, assembleDefaultClientConfigFile(clientVersion), config.TestCompareByMD5)
	assert.Equal(t, api.ExecuteSuccess, rsp6.Code.GetValue())
	assert.NotNil(t, rsp6.ConfigFile)
	assert.Equal(t, uint64(5), rsp6.ConfigFile.Version.GetValue())
	assert.Equal(t, config.CalMd5(latestContent), rsp6.ConfigFile.Md5.GetValue())
}

// TestWatchConfigFileAtFirstPublish 测试监听配置，并且第一次发布配置
func TestWatchConfigFileAtFirstPublish(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(func(cfg *testsuit.TestConfig) {
		for _, v := range cfg.Bootstrap.Logger {
			v.SetOutputLevel(commonlog.DebugLevel.Name())
		}
	}); err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	// 创建并发布配置文件
	configFile := assembleConfigFile()

	t.Run("第一次订阅发布", func(t *testing.T) {
		watchConfigFiles := assembleDefaultClientConfigFile(0)
		clientId := "TestWatchConfigFileAtFirstPublish-first"

		defer func() {
			testSuit.OriginConfigServer().WatchCenter().RemoveWatcher(clientId, watchConfigFiles)
		}()

		notifyCh := testSuit.OriginConfigServer().WatchCenter().AddWatcher(clientId, watchConfigFiles)

		rsp := testSuit.ConfigServer().CreateConfigFile(testSuit.DefaultCtx, configFile)
		t.Log("create config file success")
		assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue())

		rsp2 := testSuit.ConfigServer().PublishConfigFile(testSuit.DefaultCtx, assembleConfigFileRelease(configFile))
		t.Log("publish config file success")
		assert.Equal(t, api.ExecuteSuccess, rsp2.Code.GetValue())

		saveData, err := testSuit.Storage.GetConfigFileActiveRelease(&model.ConfigFileKey{
			Name:      configFile.GetName().GetValue(),
			Namespace: configFile.GetNamespace().GetValue(),
			Group:     configFile.GetGroup().GetValue(),
		})
		assert.NoError(t, err)
		assert.Equal(t, configFile.GetContent().GetValue(), saveData.Content)

		select {
		case notifyRsp := <-notifyCh:
			t.Logf("clientId=[%s] receive config publish msg", clientId)
			receivedVersion := notifyRsp.ConfigFile.Version.GetValue()
			assert.Equal(t, uint64(1), receivedVersion)
		case <-time.After(10 * time.Second):
			t.Fatal("time out")
		}
	})

	t.Run("第二次订阅发布", func(t *testing.T) {
		// 版本号由于发布过一次，所以是1
		watchConfigFiles := assembleDefaultClientConfigFile(1)

		clientId := "TestWatchConfigFileAtFirstPublish-second"

		notifyCh := testSuit.OriginConfigServer().WatchCenter().AddWatcher(clientId, watchConfigFiles)

		rsp3 := testSuit.ConfigServer().PublishConfigFile(testSuit.DefaultCtx, assembleConfigFileRelease(configFile))
		assert.Equal(t, api.ExecuteSuccess, rsp3.Code.GetValue())

		// 等待回调
		select {
		case notifyRsp := <-notifyCh:
			t.Logf("clientId=[%s] receive config publish msg", clientId)
			receivedVersion := notifyRsp.ConfigFile.Version.GetValue()
			assert.Equal(t, uint64(2), receivedVersion)
		case <-time.After(10 * time.Second):
			t.Fatal("time out")
		}

		// 为了避免影响其它 case，删除订阅
		testSuit.OriginConfigServer().WatchCenter().RemoveWatcher(clientId, watchConfigFiles)
	})
}

// Test10000ClientWatchConfigFile 测试 10000 个客户端同时监听配置变更，配置发布所有客户端都收到通知
func TestManyClientWatchConfigFile(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	clientSize := 100
	received := utils.NewSyncMap[string, bool]()
	receivedVersion := utils.NewSyncMap[string, uint64]()
	watchConfigFiles := assembleDefaultClientConfigFile(0)

	for i := 0; i < clientSize; i++ {
		clientId := fmt.Sprintf("Test10000ClientWatchConfigFile-client-id=%d", i)
		received.Store(clientId, false)
		receivedVersion.Store(clientId, uint64(0))
		notifyCh := testSuit.OriginConfigServer().WatchCenter().AddWatcher(clientId, watchConfigFiles)
		go func() {
			notifyRsp := <-notifyCh
			received.Store(clientId, true)
			receivedVersion.Store(clientId, notifyRsp.ConfigFile.Version.GetValue())
		}()
	}

	// 创建并发布配置文件
	configFile := assembleConfigFile()
	rsp := testSuit.ConfigServer().CreateConfigFile(testSuit.DefaultCtx, configFile)
	assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue())

	rsp2 := testSuit.ConfigServer().PublishConfigFile(testSuit.DefaultCtx, assembleConfigFileRelease(configFile))
	assert.Equal(t, api.ExecuteSuccess, rsp2.Code.GetValue())

	// 等待回调
	time.Sleep(2000 * time.Millisecond)

	// 校验是否所有客户端都收到推送通知
	receivedCnt := 0
	received.ReadRange(func(key string, val bool) {
		if val {
			receivedCnt++
		}
	})
	assert.Equal(t, received.Len(), receivedCnt)

	activeQuery := assembleConfigFileRelease(configFile)
	activeQuery.Name = nil
	activeRsp := testSuit.ConfigServer().GetConfigFileRelease(testSuit.DefaultCtx, activeQuery)
	assert.Equal(t, apimodel.Code_ExecuteSuccess, apimodel.Code(activeRsp.Code.Value), activeRsp.Info.Value)

	receivedVerCnt := 0
	receivedVersion.ReadRange(func(key string, val uint64) {
		if val == activeRsp.ConfigFileRelease.Version.Value {
			receivedVerCnt++
		}
	})
	assert.Equal(t, receivedVersion.Len(), receivedVerCnt)

	// 为了避免影响其它case，删除订阅
	received.ReadRange(func(clientId string, val bool) {
		testSuit.OriginConfigServer().WatchCenter().RemoveWatcher(clientId, watchConfigFiles)
	})
}

// TestDeleteConfigFile 测试删除配置，删除配置会通知客户端，并且重新拉取配置会返回 NotFoundResourceConfigFile 状态码
func TestDeleteConfigFile(t *testing.T) {
	testSuit := &ConfigCenterTest{}
	if err := testSuit.Initialize(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testSuit.clearTestData(); err != nil {
			t.Fatal(err)
		}
		testSuit.Destroy()
	})

	// 创建并发布一个配置文件
	configFile := assembleConfigFile()
	rsp := testSuit.ConfigServer().CreateConfigFile(testSuit.DefaultCtx, configFile)
	assert.Equal(t, api.ExecuteSuccess, rsp.Code.GetValue())

	rsp2 := testSuit.ConfigServer().PublishConfigFile(testSuit.DefaultCtx, assembleConfigFileRelease(configFile))
	assert.Equal(t, api.ExecuteSuccess, rsp2.Code.GetValue())

	time.Sleep(1200 * time.Millisecond)

	// 客户端订阅
	clientId := randomStr()
	watchConfigFiles := assembleDefaultClientConfigFile(0)

	t.Log("add config watcher")

	notifyCh := testSuit.OriginConfigServer().WatchCenter().AddWatcher(clientId, watchConfigFiles)

	// 删除配置文件
	t.Log("remove config file")
	rsp3 := testSuit.ConfigServer().DeleteConfigFile(testSuit.DefaultCtx, &apiconfig.ConfigFile{
		Namespace: utils.NewStringValue(testNamespace),
		Group:     utils.NewStringValue(testGroup),
		Name:      utils.NewStringValue(testFile),
	})
	assert.Equal(t, api.ExecuteSuccess, rsp3.Code.GetValue())

	// 客户端收到推送通知
	t.Log("wait receive config change msg")
	select {
	case notifyRsp := <-notifyCh:
		assert.Equal(t, uint64(2), notifyRsp.ConfigFile.Version.Value)
	case <-time.After(10 * time.Second):
		t.Fatal("time out")
	}

	fileInfo := &apiconfig.ClientConfigFileInfo{
		Namespace: &wrapperspb.StringValue{Value: testNamespace},
		Group:     &wrapperspb.StringValue{Value: testGroup},
		FileName:  &wrapperspb.StringValue{Value: testFile},
		Version:   &wrapperspb.UInt64Value{Value: 2},
	}

	// 重新拉取配置，获取不到配置文件
	rsp4 := testSuit.ConfigServer().GetConfigFileForClient(testSuit.DefaultCtx, fileInfo)
	assert.Equal(t, uint32(api.NotFoundResource), rsp4.Code.GetValue())
}
