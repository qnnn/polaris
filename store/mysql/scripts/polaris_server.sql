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

SET
    SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";

SET
    time_zone = "+00:00";

/*!40101 SET @OLD_CHARACTER_SET_CLIENT = @@CHARACTER_SET_CLIENT */
;

/*!40101 SET @OLD_CHARACTER_SET_RESULTS = @@CHARACTER_SET_RESULTS */
;

/*!40101 SET @OLD_COLLATION_CONNECTION = @@COLLATION_CONNECTION */
;

/*!40101 SET NAMES utf8mb4 */
;

--
-- Database: `polaris_server`
--
CREATE DATABASE IF NOT EXISTS `polaris_server` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

USE `polaris_server`;

-- --------------------------------------------------------
--
-- Table structure `business`
--
CREATE TABLE `business`
(
    `id`    varchar(32)   NOT NULL comment 'Unique ID',
    `name`  varchar(64)   NOT NULL comment 'business name',
    `token` varchar(64)   NOT NULL comment 'Token ID of the business',
    `owner` varchar(1024) NOT NULL comment 'The business is responsible for Owner',
    `flag`  tinyint(4)    NOT NULL DEFAULT '0' comment 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
    `ctime` timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime` timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `instance`
--
CREATE TABLE `instance`
(
    `id`                  varchar(128) NOT NULL comment 'Unique ID',
    `service_id`          varchar(32)  NOT NULL comment 'Service ID',
    `vpc_id`              varchar(64)           DEFAULT NULL comment 'VPC ID',
    `host`                varchar(128) NOT NULL comment 'instance Host Information',
    `port`                int(11)      NOT NULL comment 'instance port information',
    `protocol`            varchar(32)           DEFAULT NULL comment 'Listening protocols for corresponding ports, such as TPC, UDP, GRPC, DUBBO, etc.',
    `version`             varchar(32)           DEFAULT NULL comment 'The version of the instance can be used for version routing',
    `health_status`       tinyint(4)   NOT NULL DEFAULT '1' comment 'The health status of the instance, 1 is health, 0 is unhealthy',
    `isolate`             tinyint(4)   NOT NULL DEFAULT '0' comment 'Example isolation status flag, 0 is not isolated, 1 is isolated',
    `weight`              smallint(6)  NOT NULL DEFAULT '100' comment 'The weight of the instance is mainly used for LoadBalance, default is 100',
    `enable_health_check` tinyint(4)   NOT NULL DEFAULT '0' comment 'Whether to open a heartbeat on an instance, check the logic, 0 is not open, 1 is open',
    `logic_set`           varchar(128)          DEFAULT NULL comment 'Example logic packet information',
    `cmdb_region`         varchar(128)          DEFAULT NULL comment 'The region information of the instance is mainly used to close the route',
    `cmdb_zone`           varchar(128)          DEFAULT NULL comment 'The ZONE information of the instance is mainly used to close the route.',
    `cmdb_idc`            varchar(128)          DEFAULT NULL comment 'The IDC information of the instance is mainly used to close the route',
    `priority`            tinyint(4)   NOT NULL DEFAULT '0' comment 'Example priority, currently useless',
    `revision`            varchar(32)  NOT NULL comment 'Instance version information',
    `flag`                tinyint(4)   NOT NULL DEFAULT '0' comment 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
    `ctime`               timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`               timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`),
    KEY `service_id` (`service_id`),
    KEY `mtime` (`mtime`),
    KEY `host` (`host`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `health_check`
--
CREATE TABLE `health_check`
(
    `id`   varchar(128) NOT NULL comment 'Instance ID',
    `type` tinyint(4)   NOT NULL DEFAULT '0' comment 'Instance health check type',
    `ttl`  int(11)      NOT NULL comment 'TTL time jumping',
    PRIMARY KEY (`id`)
    /* CONSTRAINT `health_check_ibfk_1` FOREIGN KEY (`id`) REFERENCES `instance` (`id`) ON DELETE CASCADE ON UPDATE CASCADE */
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `instance_metadata`
--
CREATE TABLE `instance_metadata`
(
    `id`     varchar(128)  NOT NULL comment 'Instance ID',
    `mkey`   varchar(128)  NOT NULL comment 'instance label of Key',
    `mvalue` varchar(4096) NOT NULL comment 'instance label Value',
    `ctime`  timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`  timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`, `mkey`),
    KEY `mkey` (`mkey`)
    /* CONSTRAINT `instance_metadata_ibfk_1` FOREIGN KEY (`id`) REFERENCES `instance` (`id`) ON DELETE CASCADE ON UPDATE CASCADE */
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `namespace`
--
CREATE TABLE `namespace`
(
    `name`    varchar(64)   NOT NULL comment 'Namespace name, unique',
    `comment` varchar(1024)          DEFAULT NULL comment 'Description of namespace',
    `token`   varchar(64)   NOT NULL comment 'TOKEN named space for write operation check',
    `owner`   varchar(1024) NOT NULL comment 'Responsible for named space Owner',
    `flag`    tinyint(4)    NOT NULL DEFAULT '0' comment 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
    `ctime`   timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`   timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`name`)
) ENGINE = InnoDB;

--
-- Data in the conveyor `namespace`
--
INSERT INTO `namespace` (`name`,
                         `comment`,
                         `token`,
                         `owner`,
                         `flag`,
                         `ctime`,
                         `mtime`)
VALUES ('Polaris',
        'Polaris-server',
        '2d1bfe5d12e04d54b8ee69e62494c7fd',
        'polaris',
        0,
        '2019-09-06 07:55:07',
        '2019-09-06 07:55:07'),
       ('default',
        'Default Environment',
        'e2e473081d3d4306b52264e49f7ce227',
        'polaris',
        0,
        '2021-07-27 19:37:37',
        '2021-07-27 19:37:37');

-- --------------------------------------------------------
--
-- Table structure `routing_config`
--
CREATE TABLE `routing_config`
(
    `id`         varchar(32) NOT NULL comment 'Routing configuration ID',
    `in_bounds`  text comment 'Service is routing rules',
    `out_bounds` text comment 'Service main routing rules',
    `revision`   varchar(40) NOT NULL comment 'Routing rule version',
    `flag`       tinyint(4)  NOT NULL DEFAULT '0' comment 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
    `ctime`      timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`      timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `ratelimit_config`
--
CREATE TABLE `ratelimit_config`
(
    `id`         varchar(32)    NOT NULL comment 'ratelimit rule ID',
    `name`       varchar(64)    NOT NULL comment 'ratelimt rule name',
    `disable`     tinyint(4)    NOT NULL DEFAULT '0' comment 'ratelimit disable',
    `service_id` varchar(32)    NOT NULL comment 'Service ID',
    `method`     varchar(512)   NOT NULL comment 'ratelimit method',
    `labels`     text           NOT NULL comment 'Conductive flow for a specific label',
    `priority`   smallint(6)    NOT NULL DEFAULT '0' comment 'ratelimit rule priority',
    `rule`       text           NOT NULL comment 'Current limiting rules',
    `revision`   varchar(32)    NOT NULL comment 'Limiting version',
    `flag`       tinyint(4)     NOT NULL DEFAULT '0' comment 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
    `ctime`      timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`      timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    `etime`      timestamp      NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'RateLimit rule enable time',
    PRIMARY KEY (`id`),
    KEY `mtime` (`mtime`),
    KEY `service_id` (`service_id`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `ratelimit_revision`
--
CREATE TABLE `ratelimit_revision`
(
    `service_id`    varchar(32) NOT NULL comment 'Service ID',
    `last_revision` varchar(40) NOT NULL comment 'The latest limited limiting rule version of the corresponding service',
    `mtime`         timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`service_id`),
    KEY `service_id` (`service_id`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `service`
--
CREATE TABLE `service`
(
    `id`           varchar(32)   NOT NULL comment 'Service ID',
    `name`         varchar(128)  NOT NULL comment 'Service name, only under the namespace',
    `namespace`    varchar(64)   NOT NULL comment 'Namespace belongs to the service',
    `ports`        text                   DEFAULT NULL comment 'Service will have a list of all port information of the external exposure (single process exposing multiple protocols)',
    `business`     varchar(64)            DEFAULT NULL comment 'Service business information',
    `department`   varchar(1024)          DEFAULT NULL comment 'Service department information',
    `cmdb_mod1`    varchar(1024)          DEFAULT NULL comment '',
    `cmdb_mod2`    varchar(1024)          DEFAULT NULL comment '',
    `cmdb_mod3`    varchar(1024)          DEFAULT NULL comment '',
    `comment`      varchar(1024)          DEFAULT NULL comment 'Description information',
    `token`        varchar(2048) NOT NULL comment 'Service token, used to handle all the services involved in the service',
    `revision`     varchar(32)   NOT NULL comment 'Service version information',
    `owner`        varchar(1024) NOT NULL comment 'Owner information belonging to the service',
    `flag`         tinyint(4)    NOT NULL DEFAULT '0' comment 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
    `reference`    varchar(32)            DEFAULT NULL comment 'Service alias, what is the actual service name that the service is actually pointed out?',
    `refer_filter` varchar(1024)          DEFAULT NULL comment '',
    `platform_id`  varchar(32)            DEFAULT '' comment 'The platform ID to which the service belongs',
    `ctime`        timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`        timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`),
    UNIQUE KEY `name` (`name`, `namespace`),
    KEY `namespace` (`namespace`),
    KEY `mtime` (`mtime`),
    KEY `reference` (`reference`),
    KEY `platform_id` (`platform_id`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Data in the conveyor `service`
--
INSERT INTO `service` (`id`,
                       `name`,
                       `namespace`,
                       `comment`,
                       `business`,
                       `token`,
                       `revision`,
                       `owner`,
                       `flag`,
                       `ctime`,
                       `mtime`)
VALUES ('fbca9bfa04ae4ead86e1ecf5811e32a9',
        'polaris.checker',
        'Polaris',
        'polaris checker service',
        'polaris',
        '7d19c46de327408d8709ee7392b7700b',
        '301b1e9f0bbd47a6b697e26e99dfe012',
        'polaris',
        0,
        '2021-09-06 07:55:07',
        '2021-09-06 07:55:09');

-- --------------------------------------------------------
--
-- Table structure `service_metadata`
--
CREATE TABLE `service_metadata`
(
    `id`     varchar(32)   NOT NULL comment 'Service ID',
    `mkey`   varchar(128)  NOT NULL comment 'Service label key',
    `mvalue` varchar(4096) NOT NULL comment 'Service label Value',
    `ctime`  timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`  timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`, `mkey`),
    KEY `mkey` (`mkey`)
    /* CONSTRAINT `service_metadata_ibfk_1` FOREIGN KEY (`id`) REFERENCES `service` (`id`) ON DELETE CASCADE ON UPDATE CASCADE */
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `owner_service_map`Quickly query all services under an Owner
--
CREATE TABLE `owner_service_map`
(
    `id`        varchar(32)  NOT NULL comment '',
    `owner`     varchar(32)  NOT NULL comment 'Service Owner',
    `service`   varchar(128) NOT NULL comment 'service name',
    `namespace` varchar(64)  NOT NULL comment 'namespace name',
    PRIMARY KEY (`id`),
    KEY `owner` (`owner`),
    KEY `name` (`service`, `namespace`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `circuitbreaker_rule`
--
CREATE TABLE `circuitbreaker_rule`
(
    `id`         varchar(97)   NOT NULL comment 'Melting rule ID',
    `version`    varchar(32)   NOT NULL DEFAULT 'master' comment 'Melting rule version, default is MASTR',
    `name`       varchar(128)  NOT NULL comment 'Melting rule name',
    `namespace`  varchar(64)   NOT NULL comment 'Melting rule belongs to name space',
    `business`   varchar(64)            DEFAULT NULL comment 'Business information of fuse regular',
    `department` varchar(1024)          DEFAULT NULL comment 'Department information to which the fuse regular belongs',
    `comment`    varchar(1024)          DEFAULT NULL comment 'Description of the fuse rule',
    `inbounds`   text          NOT NULL comment 'Service-tuned fuse rule',
    `outbounds`  text          NOT NULL comment 'Service Motoring Fuse Rule',
    `token`      varchar(32)   NOT NULL comment 'Token, which is fucking, mainly for writing operation check',
    `owner`      varchar(1024) NOT NULL comment 'Melting rule Owner information',
    `revision`   varchar(32)   NOT NULL comment 'Melt rule version information',
    `flag`       tinyint(4)    NOT NULL DEFAULT '0' comment 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
    `ctime`      timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`      timestamp     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`, `version`),
    UNIQUE KEY `name` (`name`, `namespace`, `version`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `circuitbreaker_rule_relation`
--
CREATE TABLE `circuitbreaker_rule_relation`
(
    `service_id`   varchar(32) NOT NULL comment 'Service ID',
    `rule_id`      varchar(97) NOT NULL comment 'Melting rule ID',
    `rule_version` varchar(32) NOT NULL comment 'Melting rule version',
    `flag`         tinyint(4)  NOT NULL DEFAULT '0' comment 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
    `ctime`        timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`        timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`service_id`),
    KEY `mtime` (`mtime`),
    KEY `rule_id` (`rule_id`)
    /* CONSTRAINT `circuitbreaker_rule_relation_ibfk_1` FOREIGN KEY (`service_id`) REFERENCES `service` (`id`) ON DELETE CASCADE ON UPDATE CASCADE */
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `t_ip_config`
--
CREATE TABLE `t_ip_config`
(
    `Fip`     int(10) unsigned NOT NULL comment 'Machine IP',
    `FareaId` int(10) unsigned NOT NULL comment 'Area number',
    `FcityId` int(10) unsigned NOT NULL comment 'City number',
    `FidcId`  int(10) unsigned NOT NULL comment 'IDC number',
    `Fflag`   tinyint(4) DEFAULT '0',
    `Fstamp`  datetime         NOT NULL,
    `Fflow`   int(10) unsigned NOT NULL,
    PRIMARY KEY (`Fip`),
    KEY `idx_Fflow` (`Fflow`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `t_policy`
--
CREATE TABLE `t_policy`
(
    `FmodId` int(10) unsigned NOT NULL,
    `Fdiv`   int(10) unsigned NOT NULL,
    `Fmod`   int(10) unsigned NOT NULL,
    `Fflag`  tinyint(4) DEFAULT '0',
    `Fstamp` datetime         NOT NULL,
    `Fflow`  int(10) unsigned NOT NULL,
    PRIMARY KEY (`FmodId`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `t_route`
--
CREATE TABLE `t_route`
(
    `Fip`    int(10) unsigned NOT NULL,
    `FmodId` int(10) unsigned NOT NULL,
    `FcmdId` int(10) unsigned NOT NULL,
    `FsetId` varchar(32)      NOT NULL,
    `Fflag`  tinyint(4) DEFAULT '0',
    `Fstamp` datetime         NOT NULL,
    `Fflow`  int(10) unsigned NOT NULL,
    PRIMARY KEY (`Fip`, `FmodId`, `FcmdId`),
    KEY `Fflow` (`Fflow`),
    KEY `idx1` (`FmodId`, `FcmdId`, `FsetId`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `t_section`
--
CREATE TABLE `t_section`
(
    `FmodId` int(10) unsigned NOT NULL,
    `Ffrom`  int(10) unsigned NOT NULL,
    `Fto`    int(10) unsigned NOT NULL,
    `Fxid`   int(10) unsigned NOT NULL,
    `Fflag`  tinyint(4) DEFAULT '0',
    `Fstamp` datetime         NOT NULL,
    `Fflow`  int(10) unsigned NOT NULL,
    PRIMARY KEY (`FmodId`, `Ffrom`, `Fto`)
) ENGINE = InnoDB;

-- --------------------------------------------------------
--
-- Table structure `start_lock`
--
CREATE TABLE `start_lock`
(
    `lock_id`  int(11)     NOT NULL COMMENT '锁序号',
    `lock_key` varchar(32) NOT NULL COMMENT 'Lock name',
    `server`   varchar(32) NOT NULL COMMENT 'SERVER holding launch lock',
    `mtime`    timestamp   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Update time',
    PRIMARY KEY (`lock_id`, `lock_key`)
) ENGINE = InnoDB;

--
-- Data in the conveyor `start_lock`
--
INSERT INTO `start_lock` (`lock_id`, `lock_key`, `server`, `mtime`)
VALUES (1, 'sz', 'aaa', '2019-12-05 08:35:49');

-- --------------------------------------------------------
--
-- Table structure `cl5_module`
--
CREATE TABLE `cl5_module`
(
    `module_id`    int(11)   NOT NULL COMMENT 'Module ID',
    `interface_id` int(11)   NOT NULL COMMENT 'Interface ID',
    `range_num`    int(11)   NOT NULL,
    `mtime`        timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`module_id`)
) ENGINE = InnoDB COMMENT = 'To generate SID';

--
-- Data in the conveyor `cl5_module`
--
insert into cl5_module(module_id, interface_id, range_num) values (3000001, 1, 0);


-- --------------------------------------------------------
--
-- Table structure `config_file`
--
CREATE TABLE `config_file`
(
    `id`          bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `namespace`   varchar(64)     NOT NULL COMMENT '所属的namespace',
    `group`       varchar(128)    NOT NULL DEFAULT '' COMMENT '所属的文件组',
    `name`        varchar(128)    NOT NULL COMMENT '配置文件名',
    `content`     longtext        NOT NULL COMMENT '文件内容',
    `format`      varchar(16)              DEFAULT 'text' COMMENT '文件格式，枚举值',
    `comment`     varchar(512)             DEFAULT NULL COMMENT '备注信息',
    `flag`        tinyint(4)      NOT NULL DEFAULT '0' COMMENT '软删除标记位',
    `create_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `create_by`   varchar(32)              DEFAULT NULL COMMENT '创建人',
    `modify_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    `modify_by`   varchar(32)              DEFAULT NULL COMMENT '最后更新人',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_file` (`namespace`, `group`, `name`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1 COMMENT = '配置文件表';

-- --------------------------------------------------------
--
-- Table structure `config_file_group`
--
CREATE TABLE `config_file_group`
(
    `id`          bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `name`        varchar(128)    NOT NULL COMMENT '配置文件分组名',
    `namespace`   varchar(64)     NOT NULL COMMENT '所属的namespace',
    `comment`     varchar(512)             DEFAULT NULL COMMENT '备注信息',
    `owner`       varchar(1024)            DEFAULT NULL COMMENT '负责人',
    `create_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `create_by`   varchar(32)              DEFAULT NULL COMMENT '创建人',
    `modify_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    `modify_by`   varchar(32)              DEFAULT NULL COMMENT '最后更新人',
    `business`     varchar(64)            DEFAULT NULL comment 'Service business information',
    `department`   varchar(1024)          DEFAULT NULL comment 'Service department information',
    `metadata` text             COMMENT '配置分组标签',
    `flag`        tinyint(4)      NOT NULL DEFAULT '0' COMMENT '是否被删除',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_name` (`namespace`, `name`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1 COMMENT = '配置文件组表';

-- --------------------------------------------------------
--
-- Table structure `config_file_release`
--
CREATE TABLE `config_file_release`
(
    `id`          bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `name`        varchar(128)             DEFAULT NULL COMMENT '发布标题',
    `namespace`   varchar(64)     NOT NULL COMMENT '所属的namespace',
    `group`       varchar(128)    NOT NULL COMMENT '所属的文件组',
    `file_name`   varchar(128)    NOT NULL COMMENT '配置文件名',
    `format`      varchar(16)              DEFAULT 'text' COMMENT '文件格式，枚举值',
    `content`     longtext        NOT NULL COMMENT '文件内容',
    `comment`     varchar(512)             DEFAULT NULL COMMENT '备注信息',
    `md5`         varchar(128)    NOT NULL COMMENT 'content的md5值',
    `version`     bigint(11)         NOT NULL COMMENT '版本号，每次发布自增1',
    `flag`        tinyint(4)      NOT NULL DEFAULT '0' COMMENT '是否被删除',
    `create_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `create_by`   varchar(32)              DEFAULT NULL COMMENT '创建人',
    `modify_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    `modify_by`   varchar(32)              DEFAULT NULL COMMENT '最后更新人',
    `tags`        text             COMMENT '文件标签',
    `active`      tinyint(4)      NOT NULL DEFAULT '0' COMMENT '是否处于使用中',
    `description`  varchar(512)             DEFAULT NULL COMMENT '发布描述',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_file` (`namespace`, `group`, `file_name`, `name`),
    KEY `idx_modify_time` (`modify_time`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1 COMMENT = '配置文件发布表';

-- --------------------------------------------------------
--
-- Table structure `config_file_release_history`
--
CREATE TABLE `config_file_release_history`
(
    `id`          bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `name`        varchar(64)              DEFAULT '' COMMENT '发布名称',
    `namespace`   varchar(64)     NOT NULL COMMENT '所属的namespace',
    `group`       varchar(128)    NOT NULL COMMENT '所属的文件组',
    `file_name`   varchar(128)    NOT NULL COMMENT '配置文件名',
    `content`     longtext        NOT NULL COMMENT '文件内容',
    `format`      varchar(16)              DEFAULT 'text' COMMENT '文件格式',
    `comment`     varchar(512)             DEFAULT NULL COMMENT '备注信息',
    `md5`         varchar(128)    NOT NULL COMMENT 'content的md5值',
    `type`        varchar(32)     NOT NULL COMMENT '发布类型，例如全量发布、灰度发布',
    `status`      varchar(16)     NOT NULL DEFAULT 'success' COMMENT '发布状态，success表示成功，fail 表示失败',
    `create_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `create_by`   varchar(32)              DEFAULT NULL COMMENT '创建人',
    `modify_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    `modify_by`   varchar(32)              DEFAULT NULL COMMENT '最后更新人',
    `tags`        text             COMMENT '文件标签',
    `version`     bigint(11)        COMMENT '版本号，每次发布自增1',
    `reason`      varchar(3000)             DEFAULT '' COMMENT '原因',
    `description`  varchar(512)             DEFAULT NULL COMMENT '发布描述',
    PRIMARY KEY (`id`),
    KEY `idx_file` (`namespace`, `group`, `file_name`)
) ENGINE = InnoDB
  AUTO_INCREMENT = 1 COMMENT = '配置文件发布历史表';

-- --------------------------------------------------------
--
-- Table structure `config_file_tag`
--
CREATE TABLE `config_file_tag`
(
    `id`          bigint unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `key`         varchar(128)    NOT NULL COMMENT 'tag 的键',
    `Value`       varchar(128)    NOT NULL COMMENT 'tag 的值',
    `namespace`   varchar(64)     NOT NULL COMMENT '所属的namespace',
    `group`       varchar(128)    NOT NULL DEFAULT '' COMMENT '所属的文件组',
    `file_name`   varchar(128)    NOT NULL COMMENT '配置文件名',
    `create_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `create_by`   varchar(32)              DEFAULT NULL COMMENT '创建人',
    `modify_time` timestamp       NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    `modify_by`   varchar(32)              DEFAULT NULL COMMENT '最后更新人',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_tag` (
                         `key`,
                         `Value`,
                         `namespace`,
                         `group`,
                         `file_name`
        ),
    KEY `idx_file` (`namespace`, `group`, `file_name`)
) ENGINE = InnoDB COMMENT = '配置文件标签表';

/*!40101 SET CHARACTER_SET_CLIENT = @OLD_CHARACTER_SET_CLIENT */
;

/*!40101 SET CHARACTER_SET_RESULTS = @OLD_CHARACTER_SET_RESULTS */
;

/*!40101 SET COLLATION_CONNECTION = @OLD_COLLATION_CONNECTION */
;

CREATE TABLE `user`
(
    `id`           VARCHAR(128) NOT NULL comment 'User ID',
    `name`         VARCHAR(100) NOT NULL comment 'user name',
    `password`     VARCHAR(100) NOT NULL comment 'user password',
    `owner`        VARCHAR(128) NOT NULL comment 'Main account ID',
    `source`       VARCHAR(32)  NOT NULL comment 'Account source',
    `mobile`       VARCHAR(12)  NOT NULL DEFAULT '' comment 'Account mobile phone number',
    `email`        VARCHAR(64)  NOT NULL DEFAULT '' comment 'Account mailbox',
    `token`        VARCHAR(255) NOT NULL comment 'The token information owned by the account can be used for SDK access authentication',
    `token_enable` tinyint(4)   NOT NULL DEFAULT 1,
    `user_type`    int          NOT NULL DEFAULT 20 comment 'Account type, 0 is the admin super account, 20 is the primary account, 50 for the child account',
    `comment`      VARCHAR(255) NOT NULL comment 'describe',
    `flag`         tinyint(4)   NOT NULL DEFAULT '0' COMMENT 'Whether the rules are valid, 0 is valid, 1 is invalid, it is deleted',
    `ctime`        timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`        timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`, `owner`),
    KEY `owner` (`owner`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

CREATE TABLE `user_group`
(
    `id`           VARCHAR(128) NOT NULL comment 'User group ID',
    `name`         VARCHAR(100) NOT NULL comment 'User group name',
    `owner`        VARCHAR(128) NOT NULL comment 'The main account ID of the user group',
    `token`        VARCHAR(255) NOT NULL comment 'TOKEN information of this user group',
    `comment`      VARCHAR(255) NOT NULL comment 'Description',
    `token_enable` tinyint(4)   NOT NULL DEFAULT 1,
    `flag`         tinyint(4)   NOT NULL DEFAULT '0' COMMENT 'Whether the rules are valid, 0 is valid, 1 is invalid, it is deleted',
    `ctime`        timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`        timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`, `owner`),
    KEY `owner` (`owner`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

CREATE TABLE `user_group_relation`
(
    `user_id`  VARCHAR(128) NOT NULL comment 'User ID',
    `group_id` VARCHAR(128) NOT NULL comment 'User group ID',
    `ctime`    timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`    timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`user_id`, `group_id`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

CREATE TABLE `auth_strategy`
(
    `id`       VARCHAR(128) NOT NULL comment 'Strategy ID',
    `name`     VARCHAR(100) NOT NULL comment 'Policy name',
    `action`   VARCHAR(32)  NOT NULL comment 'Read and write permission for this policy, only_read = 0, read_write = 1',
    `owner`    VARCHAR(128) NOT NULL comment 'The account ID to which this policy is',
    `comment`  VARCHAR(255) NOT NULL comment 'describe',
    `default`  tinyint(4)   NOT NULL DEFAULT '0',
    `revision` VARCHAR(128) NOT NULL comment 'Authentication rule version',
    `flag`     tinyint(4)   NOT NULL DEFAULT '0' COMMENT 'Whether the rules are valid, 0 is valid, 1 is invalid, it is deleted',
    `ctime`    timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`    timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`id`),
    UNIQUE KEY (`name`, `owner`),
    KEY `owner` (`owner`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

CREATE TABLE `auth_principal`
(
    `strategy_id`    VARCHAR(128) NOT NULL comment 'Strategy ID',
    `principal_id`   VARCHAR(128) NOT NULL comment 'Principal ID',
    `principal_role` int          NOT NULL comment 'PRINCIPAL type, 1 is User, 2 is Group',
    PRIMARY KEY (`strategy_id`, `principal_id`, `principal_role`)
) ENGINE = InnoDB;

CREATE TABLE `auth_strategy_resource`
(
    `strategy_id` VARCHAR(128) NOT NULL comment 'Strategy ID',
    `res_type`    int          NOT NULL comment 'Resource Type, Namespaces = 0, Service = 1, configgroups = 2',
    `res_id`      VARCHAR(128) NOT NULL comment 'Resource ID',
    `ctime`       timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'Create time',
    `mtime`       timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'Last updated time',
    PRIMARY KEY (`strategy_id`, `res_type`, `res_id`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

-- Create a default master account, password is Polarismesh @ 2021
INSERT INTO `user` (`id`,
                    `name`,
                    `password`,
                    `source`,
                    `token`,
                    `token_enable`,
                    `user_type`,
                    `comment`,
                    `mobile`,
                    `email`,
                    `owner`)
VALUES ('65e4789a6d5b49669adf1e9e8387549c',
        'polaris',
        '$2a$10$3izWuZtE5SBdAtSZci.gs.iZ2pAn9I8hEqYrC6gwJp1dyjqQnrrum',
        'Polaris',
        'nu/0WRA4EqSR1FagrjRj0fZwPXuGlMpX+zCuWu4uMqy8xr1vRjisSbA25aAC3mtU8MeeRsKhQiDAynUR09I=',
        1,
        20,
        'default polaris admin account',
        '12345678910',
                '12345678910',
        '');

-- Permissions policy inserted into Polaris-Admin
INSERT INTO `auth_strategy`(`id`,
                            `name`,
                            `action`,
                            `owner`,
                            `comment`,
                            `default`,
                            `revision`,
                            `flag`,
                            `ctime`,
                            `mtime`)
VALUES ('fbca9bfa04ae4ead86e1ecf5811e32a9',
        '(用户) polaris的默认策略',
        'READ_WRITE',
        '65e4789a6d5b49669adf1e9e8387549c',
        'default admin',
        1,
        'fbca9bfa04ae4ead86e1ecf5811e32a9',
        0,
        sysdate(),
        sysdate());

-- Sport rules inserted into Polaris-Admin to access
INSERT INTO `auth_strategy_resource`(`strategy_id`,
                                     `res_type`,
                                     `res_id`,
                                     `ctime`,
                                     `mtime`)
VALUES ('fbca9bfa04ae4ead86e1ecf5811e32a9',
        0,
        '*',
        sysdate(),
        sysdate()),
       ('fbca9bfa04ae4ead86e1ecf5811e32a9',
        1,
        '*',
        sysdate(),
        sysdate()),
       ('fbca9bfa04ae4ead86e1ecf5811e32a9',
        2,
        '*',
        sysdate(),
        sysdate());

-- Insert permission policies and association relationships for Polaris-Admin accounts
INSERT INTO auth_principal(`strategy_id`, `principal_id`, `principal_role`) VALUE (
                                                                                   'fbca9bfa04ae4ead86e1ecf5811e32a9',
                                                                                   '65e4789a6d5b49669adf1e9e8387549c',
                                                                                   1
    );

-- v1.8.0, support client info storage
CREATE TABLE `client`
(
    `id`      VARCHAR(128) NOT NULL comment 'client id',
    `host`    VARCHAR(100) NOT NULL comment 'client host IP',
    `type`    VARCHAR(100) NOT NULL comment 'client type: polaris-java/polaris-go',
    `version` VARCHAR(32)  NOT NULL comment 'client SDK version',
    `region`  varchar(128)          DEFAULT NULL comment 'region info for client',
    `zone`    varchar(128)          DEFAULT NULL comment 'zone info for client',
    `campus`  varchar(128)          DEFAULT NULL comment 'campus info for client',
    `flag`    tinyint(4)   NOT NULL DEFAULT '0' COMMENT '0 is valid, 1 is invalid(deleted)',
    `ctime`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP comment 'create time',
    `mtime`   timestamp    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP comment 'last updated time',
    PRIMARY KEY (`id`),
    KEY `mtime` (`mtime`)
) ENGINE = InnoDB;

CREATE TABLE `client_stat`
(
    `client_id` VARCHAR(128) NOT NULL comment 'client id',
    `target`    VARCHAR(100) NOT NULL comment 'target stat platform',
    `port`      int(11)      NOT NULL comment 'client port to get stat information',
    `protocol`  VARCHAR(100) NOT NULL comment 'stat info transport protocol',
    `path`      VARCHAR(128) NOT NULL comment 'stat metric path',
    PRIMARY KEY (`client_id`, `target`, `port`)
) ENGINE = InnoDB;

-- v1.9.0
CREATE TABLE `config_file_template` (
    `id` bigint(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
    `name` varchar(128) COLLATE utf8_bin NOT NULL COMMENT '配置文件模板名称',
    `content` longtext COLLATE utf8_bin NOT NULL COMMENT '配置文件模板内容',
    `format` varchar(16) COLLATE utf8_bin DEFAULT 'text' COMMENT '模板文件格式',
    `comment` varchar(512) COLLATE utf8_bin DEFAULT NULL COMMENT '模板描述信息',
    `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `create_by` varchar(32) COLLATE utf8_bin DEFAULT NULL COMMENT '创建人',
    `modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '最后更新时间',
    `modify_by` varchar(32) COLLATE utf8_bin DEFAULT NULL COMMENT '最后更新人',
    PRIMARY KEY (`id`),
    UNIQUE KEY `uk_name` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COLLATE=utf8_bin COMMENT='配置文件模板表';

INSERT INTO `config_file_template` (`name`, `content`, `format`, `comment`, `create_time`
	, `create_by`, `modify_time`, `modify_by`)
VALUES ("spring-cloud-gateway-braining", '{
    "rules":[
        {
            "conditions":[
                {
                    "key":"${http.query.uid}",
                    "values":["10000"],
                    "operation":"EQUALS"
                }
            ],
            "labels":[
                {
                    "key":"env",
                    "value":"green"
                }
            ]
        }
    ]
}', "json", "Spring Cloud Gateway  染色规则", NOW()
	, "polaris", NOW(), "polaris");

-- v1.12.0
CREATE TABLE `routing_config_v2`
(
    `id`       VARCHAR(128) NOT NULL,
    `name`     VARCHAR(64) NOT NULL default '',
    `namespace`     VARCHAR(64) NOT NULL default '',
    `policy`   VARCHAR(64) NOT NULL,
    `config`   TEXT,
    `enable`   INT         NOT NULL DEFAULT 0,
    `revision` VARCHAR(40) NOT NULL,
    `description` VARCHAR(500) NOT NULL DEFAULT '',
    `priority`   smallint(6)    NOT NULL DEFAULT '0' comment 'ratelimit rule priority',
    `flag`     TINYINT(4)  NOT NULL DEFAULT '0',
    `ctime`    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `mtime`    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `etime`    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `extend_info` VARCHAR(1024) DEFAULT '',
    PRIMARY KEY (`id`),
    KEY `mtime` (`mtime`)
) engine = innodb;

CREATE TABLE `leader_election`
(
    `elect_key` VARCHAR(128) NOT NULL,
    `version`   BIGINT NOT NULL DEFAULT 0,
    `leader`    VARCHAR(128) NOT NULL,
    `ctime`     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `mtime`     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`elect_key`),
	KEY `version` (`version`)
) engine = innodb;

-- v1.14.0
CREATE TABLE `circuitbreaker_rule_v2`
(
    `id`                VARCHAR(128) NOT NULL,
    `name`              VARCHAR(64)  NOT NULL,
    `namespace`         VARCHAR(64)  NOT NULL default '',
    `enable`            INT          NOT NULL DEFAULT 0,
    `revision`          VARCHAR(40)  NOT NULL,
    `description`       VARCHAR(1024) NOT NULL DEFAULT '',
    `level`             INT          NOT NULL,
    `src_service`        VARCHAR(128) NOT NULL,
    `src_namespace`      VARCHAR(64)  NOT NULL,
    `dst_service`        VARCHAR(128) NOT NULL,
    `dst_namespace`      VARCHAR(64)  NOT NULL,
    `dst_method`         VARCHAR(128) NOT NULL,
    `config`            TEXT,
    `flag`              TINYINT(4)   NOT NULL DEFAULT '0',
    `ctime`             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `mtime`             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    `etime`             TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `name` (`name`),
    KEY `mtime` (`mtime`)
) engine = innodb;

CREATE TABLE `fault_detect_rule`
(
    `id`            VARCHAR(128) NOT NULL,
    `name`          VARCHAR(64)  NOT NULL,
    `namespace`     VARCHAR(64)  NOT NULL default 'default',
    `revision`      VARCHAR(40)  NOT NULL,
    `description`   VARCHAR(1024) NOT NULL DEFAULT '',
    `dst_service`    VARCHAR(128) NOT NULL,
    `dst_namespace`  VARCHAR(64)  NOT NULL,
    `dst_method`     VARCHAR(128) NOT NULL,
    `config`        TEXT,
    `flag`          TINYINT(4)   NOT NULL DEFAULT '0',
    `ctime`         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `mtime`         TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    KEY `name` (`name`),
    KEY `mtime` (`mtime`)
) engine = innodb;
