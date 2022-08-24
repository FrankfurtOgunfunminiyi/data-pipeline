GO

IF EXISTS (  SELECT 1 
             FROM master.dbo.sysprocesses 
             WHERE program_name = N'SQLAgent - Generic Refresher')
BEGIN
    SELECT @@SERVERNAME AS 'InstanceName', 1 AS 'SQLServerAgentRunning'
END
ELSE 
BEGIN
    SELECT @@SERVERNAME AS 'InstanceName', 0 AS 'SQLServerAgentRunning'
END  


GO
EXEC sys.sp_cdc_enable_db


GO

EXEC sys.sp_cdc_enable_table
@source_schema = N'dbo',
@source_name   = N'dbo.TruncateTest1',
@role_name     = N'NULL'


GO

EXEC sys.sp_cdc_help_change_data_capture
GO




{
    "name": "debazium-sqlserver-connector", 
    "config": {
        "connector.class": "io.debezium.connector.sqlserver.SqlServerConnector", 
        "database.hostname": "3.141.28.175", 
        "database.port": "1433", 
        "database.user": "KafkaTest", 
        "database.password": "egcW9fAT7Qr9REPV", 
        "database.dbname": "Analytics_RiskMgmt_Test", 
        "database.server.name": "MP2-HOU-SQL-01", 
        "table.whitelist": "dbo.TruncateTest1",
        "database.history.kafka.bootstrap.servers": "3.141.28.175:9092", 
        "database.history.kafka.topic": "MP2-HOU-SQL-01.Analytics_RiskMgmt_Test",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter.schema.registry.url": "http://3.141.28.175:8081",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://3.141.28.175:8081",
        "value.converter.schemas.enable": "true"  
    }
}

