package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.DATABASE_NAME;
import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.getDefaultProperties;
import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.getRandomTableName;
import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.getTableEnvironment;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

import io.tidb.bigdata.flink.connector.catalog.TiDBCatalog;
import io.tidb.bigdata.flink.connector.source.TiDBOptions;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;


public class FlinkTableTest {
  
  /**
   * only test for timestamp
   */
  @Test
  public void testTableFactory() throws Exception {
    // only test for timestamp
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    Map<String, String> properties = getDefaultProperties();
    properties.put("connector", "tidb");
    properties.put(TiDBOptions.DATABASE_NAME.key(), "test");
    properties.put(TiDBOptions.TABLE_NAME.key(), "test_timestamp");
    properties.put("tidb.timestamp-format.c1", "yyyy-MM-dd HH:mm:ss");
    properties.put("tidb.timestamp-format.c2", "yyyy-MM-dd HH:mm:ss");
    // create test database and table
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    tiDBCatalog.sqlUpdate("DROP TABLE IF EXISTS `test_timestamp`");
    tiDBCatalog.sqlUpdate("CREATE TABLE `test_timestamp`(`c1` VARCHAR(255), `c2` timestamp)",
        "INSERT INTO `test_timestamp` VALUES('2020-01-01 12:00:01','2020-01-01 12:00:02')");
    String propertiesString = properties.entrySet().stream()
        .map(entry -> format("'%s' = '%s'", entry.getKey(), entry.getValue())).collect(
            Collectors.joining(",\n"));
    String createTableSql = format(
        "CREATE TABLE `test_timestamp`(`c1` timestamp, `c2` string) WITH (\n%s\n)",
        propertiesString);
    tableEnvironment.executeSql(createTableSql);
    Row row = tableEnvironment.executeSql("SELECT * FROM `test_timestamp`").collect().next();
    Row row1 = new Row(2);
    row1.setField(0, LocalDateTime.of(2020, 1, 1, 12, 0, 1));
    row1.setField(1, "2020-01-01 12:00:02");
    assertEquals(row, row1);
  
    tableEnvironment.executeSql("DROP TABLE `test_timestamp`");
    createTableSql = format(
        "CREATE TABLE `test_timestamp`(`c2` string) WITH (\n%s\n)", propertiesString);
    tableEnvironment.executeSql(createTableSql);
    row = tableEnvironment.executeSql("SELECT * FROM `test_timestamp`").collect().next();
    row1 = new Row(1);
    row1.setField(0, "2020-01-01 12:00:02");
    assertEquals(row, row1);
    tiDBCatalog.close();
  }
  
  
  @Test
  public void testLookupTableSource() throws Exception {
    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner()
        .inStreamingMode().build();
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);
    Map<String, String> properties = getDefaultProperties();
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    String tableName = getRandomTableName();
    String createTableSql1 = String
        .format("CREATE TABLE `%s`.`%s` (c1 int, c2 varchar(255), PRIMARY KEY(`c1`))",
            DATABASE_NAME, tableName);
    String insertDataSql = String
        .format("INSERT INTO `%s`.`%s` VALUES (1,'data1'),(2,'data2'),(3,'data3'),(4,'data4')",
            DATABASE_NAME, tableName);
    tiDBCatalog.sqlUpdate(createTableSql1, insertDataSql);
    tableEnvironment.registerCatalog("tidb", tiDBCatalog);
    List<Row> rows = IntStream.range(1, 11).mapToObj(Row::of)
        .collect(Collectors.toList());
    TableSchema tableSchema = TableSchema.builder().field("c1", DataTypes.INT().notNull()).build();
    Table table = tableEnvironment.fromValues(tableSchema.toRowDataType(), rows);
    tableEnvironment.registerTable("data", table);
    String sql = String.format(
        "SELECT * FROM (SELECT c1,PROCTIME() AS proctime FROM data) AS `datagen` "
            + "LEFT JOIN `%s`.`%s`.`%s` FOR SYSTEM_TIME AS OF datagen.proctime AS `dim_table` "
            + "ON datagen.c1 = dim_table.c1 ",
        "tidb", DATABASE_NAME, tableName);
    CloseableIterator<Row> iterator = tableEnvironment.executeSql(sql).collect();
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Object c1 = row.getField(0);
      String c2 = String.format("data%s", c1);
      boolean isJoin = (int) c1 <= 4;
      Row row1 = Row.of(c1, row.getField(1), isJoin ? c1 : null, isJoin ? c2 : null);
      assertEquals(row, row1);
    }
  }
}
