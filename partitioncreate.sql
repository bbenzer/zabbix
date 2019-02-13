DROP PROCEDURE partition_verify;
DROP PROCEDURE partition_create;
DROP PROCEDURE partition_drop;
DROP PROCEDURE partition_maintenance;


DELIMITER $$
CREATE PROCEDURE `partition_create`(zabbix varchar(64), history varchar(64), part varchar(64), CLOCK int)
BEGIN
        DECLARE RETROWS INT;
        SELECT COUNT(1) INTO RETROWS
        FROM information_schema.partitions
        WHERE table_schema = zabbix AND table_name = history AND partition_description >= CLOCK;
        IF RETROWS = 0 THEN
                SELECT CONCAT( "partition_create(", zabbix, ",", history, ",", part, ",", CLOCK, ")" ) AS msg;
                SET @sql = CONCAT( 'ALTER TABLE ', zabbix, '.', history, ' ADD PARTITION (PARTITION ', part, ' VALUES LESS THAN (', CLOCK, '));' );
                PREPARE STMT FROM @sql;
                EXECUTE STMT;
                DEALLOCATE PREPARE STMT;
        END IF;
END$$
DELIMITER ;
DELIMITER $$



CREATE PROCEDURE `partition_drop`(zabbix VARCHAR(64), history VARCHAR(64), DELETE_BELOW_PARTITION_DATE BIGINT)
BEGIN
        DECLARE done INT DEFAULT FALSE;
        DECLARE drop_part_name VARCHAR(16);
     
        DECLARE myCursor CURSOR FOR
                SELECT partition_name
                FROM information_schema.partitions
                WHERE table_schema = zabbix AND table_name = history AND CAST(SUBSTRING(partition_name FROM 2) AS UNSIGNED) < DELETE_BELOW_PARTITION_DATE;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
        SET @alter_header = CONCAT("ALTER TABLE ", zabbix, ".", history, " DROP PARTITION ");
        SET @drop_partitions = "";
        OPEN myCursor;
        read_loop: LOOP
                FETCH myCursor INTO drop_part_name;
                IF done THEN
                        LEAVE read_loop;
                END IF;
                SET @drop_partitions = IF(@drop_partitions = "", drop_part_name, CONCAT(@drop_partitions, ",", drop_part_name));
        END LOOP;
        IF @drop_partitions != "" THEN
                SET @full_sql = CONCAT(@alter_header, @drop_partitions, ";");
                PREPARE STMT FROM @full_sql;
                EXECUTE STMT;
                DEALLOCATE PREPARE STMT;
                SELECT CONCAT(zabbix, ".", history) AS `table`, @drop_partitions AS `partitions_deleted`;
        ELSE
                SELECT CONCAT(zabbix, ".", history) AS `table`, "N/A" AS `partitions_deleted`;
        END IF;
END$$
DELIMITER ;
DELIMITER $$



CREATE PROCEDURE `partition_maintenance`(zabbix VARCHAR(32), history VARCHAR(32), KEEP_DATA_DAYS INT, HOURLY_INTERVAL INT, CREATE_NEXT_INTERVALS INT)
BEGIN
        DECLARE OLDER_THAN_PARTITION_DATE VARCHAR(16);
        DECLARE PARTITION_NAME VARCHAR(16);
        DECLARE OLD_PARTITION_NAME VARCHAR(16);
        DECLARE LESS_THAN_TIMESTAMP INT;
        DECLARE CUR_TIME INT;
        CALL partition_verify(zabbix, history, HOURLY_INTERVAL);
        SET CUR_TIME = UNIX_TIMESTAMP(DATE_FORMAT(NOW(), '%Y-%m-%d 00:00:00'));
        SET @__interval = 1;
        create_loop: LOOP
                IF @__interval > CREATE_NEXT_INTERVALS THEN
                        LEAVE create_loop;
                END IF;
                SET LESS_THAN_TIMESTAMP = CUR_TIME + (HOURLY_INTERVAL * @__interval * 3600);
                SET PARTITION_NAME = FROM_UNIXTIME(CUR_TIME + HOURLY_INTERVAL * (@__interval - 1) * 3600, 'p%Y%m%d%H00');
                IF(PARTITION_NAME != OLD_PARTITION_NAME) THEN
                        CALL partition_create(zabbix, history,  PARTITION_NAME, LESS_THAN_TIMESTAMP);
                END IF;
                SET @__interval=@__interval+1;
                SET OLD_PARTITION_NAME = PARTITION_NAME;
        END LOOP;
        SET OLDER_THAN_PARTITION_DATE=DATE_FORMAT(DATE_SUB(NOW(), INTERVAL KEEP_DATA_DAYS DAY), '%Y%m%d0000');
        CALL partition_drop(zabbix, history, OLDER_THAN_PARTITION_DATE);
END$$
DELIMITER ;
DELIMITER $$


CREATE PROCEDURE `partition_verify`(zabbix VARCHAR(64), history VARCHAR(64), HOURLYINTERVAL INT(11))
BEGIN
        DECLARE PARTITION_NAME VARCHAR(16);
        DECLARE RETROWS INT(11);
        DECLARE FUTURE_TIMESTAMP TIMESTAMP;
        SELECT COUNT(1) INTO RETROWS
        FROM information_schema.partitions
        WHERE table_schema = zabbix AND table_name = history AND partition_name IS NULL;
        IF RETROWS = 1 THEN
                SET FUTURE_TIMESTAMP = TIMESTAMPADD(HOUR, HOURLYINTERVAL, CONCAT(CURDATE(), " ", '00:00:00'));
                SET PARTITION_NAME = DATE_FORMAT(CURDATE(), 'p%Y%m%d%H00');
                
                SET @__PARTITION_SQL = CONCAT("ALTER TABLE ", zabbix, ".", history, " PARTITION BY RANGE(`clock`)");
                SET @__PARTITION_SQL = CONCAT(@__PARTITION_SQL, "(PARTITION ", PARTITION_NAME, " VALUES LESS THAN (", UNIX_TIMESTAMP(FUTURE_TIMESTAMP), "));");
                
                PREPARE STMT FROM @__PARTITION_SQL;
                EXECUTE STMT;
                DEALLOCATE PREPARE STMT;
        END IF;
END$$
DELIMITER ;
