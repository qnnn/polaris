/*
 Navicat Premium Data Transfer

 Source Server         : kimikimi
 Source Server Type    : MySQL
 Source Server Version : 80021
 Source Host           : localhost:3306
 Source Schema         : polaris_server

 Target Server Type    : MySQL
 Target Server Version : 80021
 File Encoding         : 65001

 Date: 10/09/2023 13:33:14
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for instance_v2
-- ----------------------------
DROP TABLE IF EXISTS `instance_v2`;
CREATE TABLE `instance_v2`  (
  `id` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'Unique ID',
  `service_id` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'Service ID',
  `vpc_id` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'VPC ID',
  `host` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'instance Host Information',
  `port` int NOT NULL COMMENT 'instance port information',
  `protocol` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'Listening protocols for corresponding ports, such as TPC, UDP, GRPC, DUBBO, etc.',
  `version` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'The version of the instance can be used for version routing',
  `health_status` tinyint NOT NULL DEFAULT 1 COMMENT 'The health status of the instance, 1 is health, 0 is unhealthy',
  `isolate` tinyint NOT NULL DEFAULT 0 COMMENT 'Example isolation status flag, 0 is not isolated, 1 is isolated',
  `weight` smallint NOT NULL DEFAULT 100 COMMENT 'The weight of the instance is mainly used for LoadBalance, default is 100',
  `enable_health_check` tinyint NOT NULL DEFAULT 0 COMMENT 'Whether to open a heartbeat on an instance, check the logic, 0 is not open, 1 is open',
  `logic_set` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'Example logic packet information',
  `cmdb_region` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'The region information of the instance is mainly used to close the route',
  `cmdb_zone` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'The ZONE information of the instance is mainly used to close the route.',
  `cmdb_idc` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'The IDC information of the instance is mainly used to close the route',
  `priority` tinyint NOT NULL DEFAULT 0 COMMENT 'Example priority, currently useless',
  `revision` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL COMMENT 'Instance version information',
  `flag` tinyint NOT NULL DEFAULT 0 COMMENT 'Logic delete flag, 0 means visible, 1 means that it has been logically deleted',
  `health_check_type` tinyint NULL DEFAULT NULL COMMENT 'instance health check type',
  `ttl` int NULL DEFAULT NULL COMMENT 'TTL time jumping',
  `metadata_type` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL DEFAULT NULL COMMENT 'json or encode',
  `metadata` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NULL COMMENT 'metadata',
  `ctime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) COMMENT 'Create time',
  `mtime` timestamp(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0) ON UPDATE CURRENT_TIMESTAMP(0) COMMENT 'Last updated time',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `service_id`(`service_id`) USING BTREE,
  INDEX `mtime`(`mtime`) USING BTREE,
  INDEX `host`(`host`) USING BTREE
) ENGINE = InnoDB CHARACTER SET = utf8mb4 COLLATE = utf8mb4_bin ROW_FORMAT = DYNAMIC;

SET FOREIGN_KEY_CHECKS = 1;
