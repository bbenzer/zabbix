CREATE DEFINER=`root`@`%` PROCEDURE `delete_log_history`(IN interval_date INTEGER (11))
BEGIN
    
	IF interval_date IS NULL OR interval_date < 7 THEN
		SET interval_date := 49;
	ELSE 
		SET interval_date := interval_date - 1;
	END IF;
    
	SET @partition_names := (SELECT GROUP_CONCAT(PARTITION_NAME)
		FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE TABLE_NAME ='history'
		AND PARTITION_DESCRIPTION < UNIX_TIMESTAMP(CURRENT_DATE - INTERVAL interval_date DAY));
	IF @partition_names IS NOT NULL THEN
		SET @ddl := CONCAT('ALTER TABLE history DROP PARTITION ', @partition_names);
		PREPARE stmt FROM @ddl;
		EXECUTE stmt;
    END IF;
    
    SET @partition_names := (SELECT GROUP_CONCAT(PARTITION_NAME)
		FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE TABLE_NAME ='history_uint'
		AND PARTITION_DESCRIPTION < UNIX_TIMESTAMP(CURRENT_DATE - INTERVAL interval_date DAY));
	IF @partition_names IS NOT NULL THEN        
		SET @ddl := CONCAT('ALTER TABLE history_uint DROP PARTITION ', @partition_names);
		PREPARE stmt FROM @ddl;
		EXECUTE stmt;
	END IF;
    
    /*
    SET @partition_names := (SELECT GROUP_CONCAT(PARTITION_NAME)
		FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE TABLE_NAME ='trends_uint'
		AND PARTITION_DESCRIPTION < UNIX_TIMESTAMP(CURRENT_DATE - INTERVAL interval_date DAY));
	IF @partition_names IS NOT NULL THEN    
		SET @ddl := CONCAT('ALTER TABLE trends_uint DROP PARTITION ', @partition_names);
		PREPARE stmt FROM @ddl;
		EXECUTE stmt;
	END IF;        
    */

END
