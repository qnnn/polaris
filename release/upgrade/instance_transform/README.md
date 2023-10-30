## 服务实例迁移工具

### 说明

本工具主要用于将存量的服务实例迁移到1.18.x及以上版本的服务实例。

### 使用方式

***step1: 编译***

将工具进行编译，再此目录下执行```go build```可完成迁移。

***step2: 升级数据库***

执行数据库升级脚本，将数据库升级到1.18.x及以上版本的数据库，确保数据库中instance数据库表存在health_check_type、health_check_ttl和metadata字段。

***step3: 执行迁移***

执行迁移工具，并输入数据库的地址及用户名等信息，如下所示：

```shell
./instance_transform --db_addr=127.0.0.1:3306 --db_name=polaris_server --db_user=root --db_pwd=123456
```