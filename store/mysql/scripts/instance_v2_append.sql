ALTER TABLE `polaris_server`.`instance`
ADD COLUMN `health_check_type` tinyint NULL COMMENT 'Instance health check type' AFTER `flag`,
ADD COLUMN `health_check_ttl` int NULL COMMENT 'TTL time jumping' AFTER `health_check_type`,
ADD COLUMN `metadata` text NULL COMMENT 'Instance Label' AFTER `health_check_ttl`;