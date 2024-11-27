首先需创建资产表,执行resource目录下的sql语句
入口函数在类:com.lakesoul.Asstes.PostgresPartitionProcessing 
代码执行示例:
$FLINK_HOME/bin run -c com.lakesoul.Asstes.PostgresPartitionProcessing lakesoulAssets.jar \
--source_db.db_name lakesoul_test \
--source_db.user lakesoul_test \
--source_db.password lakesoul_test \
--source_db.host localhost \
--source_db.port 5432 \
--slotName flink \
--schemaList public \
--plugName pgoutput \
