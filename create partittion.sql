CREATE DEFINER=`root`@`%` PROCEDURE `event_auto_partition_sp`()
BEGIN
CALL partition_maintenance(zabbix, 'history', 50, 1, 1944);
CALL partition_maintenance(zabbix, 'history_uint', 50, 1, 1944);
CALL partition_maintenance(zabbix, 'history_log', 50, 1, 1944);
CALL partition_maintenance(zabbix, 'trends', 50, 1, 1944);
CALL partition_maintenance(zabbix, 'trends_uint', 50, 1, 1944);
END 
