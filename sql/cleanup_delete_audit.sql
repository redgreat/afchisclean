-- 每日清理任务审计表（在业务库或独立库执行一次）
CREATE TABLE IF NOT EXISTS `cleanup_delete_audit` (
  `Id` bigint(20) NOT NULL AUTO_INCREMENT,
  `RunDate` date NOT NULL COMMENT '任务运行日期',
  `HandlerName` varchar(64) NOT NULL COMMENT '处理器类名',
  `TableName` varchar(128) NOT NULL COMMENT '被删除的表',
  `DeletedCount` int(11) NOT NULL DEFAULT '0' COMMENT '本批删除行数',
  `ServiceProviderCode` varchar(32) DEFAULT NULL COMMENT '服务商编码筛选条件',
  `MinWorkOrderDate` date DEFAULT NULL COMMENT '本批关联工单最小创建日期',
  `MaxWorkOrderDate` date DEFAULT NULL COMMENT '本批关联工单最大创建日期',
  `BatchNo` int(11) DEFAULT NULL COMMENT '当日批次序号',
  `Remark` varchar(500) DEFAULT NULL,
  `CreatedAt` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`Id`),
  KEY `idx_run_handler` (`RunDate`, `HandlerName`),
  KEY `idx_table` (`TableName`, `RunDate`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='历史数据清理删除审计';
