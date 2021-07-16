package io.tidb.bigdata.flink.tidb;

import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.CATALOG_NAME;
import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.DATABASE_NAME;
import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.getDefaultProperties;
import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.getRandomTableName;
import static io.tidb.bigdata.flink.tidb.FlinkTestHelper.getTableEnvironment;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_REPLICA_READ;
import static io.tidb.bigdata.tidb.ClientConfig.TIDB_WRITE_MODE;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import io.tidb.bigdata.flink.connector.catalog.TiDBCatalog;
import java.util.Arrays;
import java.util.Map;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.Test;

public class FlinkCatalogTest {
  
  public static final String CREATE_DATABASE_SQL = "CREATE DATABASE IF NOT EXISTS `test`";
  
  public static final String DROP_TABLE_SQL_FORMAT = "DROP TABLE IF EXISTS `%s`.`%s`";
  
  public static final String CREATE_TABLE_SQL_FORMAT =
      "CREATE TABLE IF NOT EXISTS `%s`.`%s`\n"
          + "(\n"
          + "    c1  tinyint,\n"
          + "    c2  smallint,\n"
          + "    c3  mediumint,\n"
          + "    c4  int,\n"
          + "    c5  bigint,\n"
          + "    c6  char(10),\n"
          + "    c7  varchar(20),\n"
          + "    c8  tinytext,\n"
          + "    c9  mediumtext,\n"
          + "    c10 text,\n"
          + "    c11 longtext,\n"
          + "    c12 binary(20),\n"
          + "    c13 varbinary(20),\n"
          + "    c14 tinyblob,\n"
          + "    c15 mediumblob,\n"
          + "    c16 blob,\n"
          + "    c17 longblob,\n"
          + "    c18 float,\n"
          + "    c19 double,\n"
          + "    c20 decimal(6, 3),\n"
          + "    c21 date,\n"
          + "    c22 time,\n"
          + "    c23 datetime,\n"
          + "    c24 timestamp,\n"
          + "    c25 year,\n"
          + "    c26 boolean,\n"
          + "    c27 json,\n"
          + "    c28 enum ('1','2','3'),\n"
          + "    c29 set ('a','b','c'),\n"
          + "    PRIMARY KEY(c1),\n"
          + "    UNIQUE KEY(c2)\n"
          + ")";
  
  // for write mode, only unique key and primary key is mutable.
  public static final String INSERT_ROW_SQL_FORMAT =
      "INSERT INTO `%s`.`%s`.`%s`\n"
          + "VALUES (\n"
          + " cast(%s as tinyint) ,\n"
          + " cast(%s as smallint) ,\n"
          + " cast(1 as int) ,\n"
          + " cast(1 as int) ,\n"
          + " cast(1 as bigint) ,\n"
          + " cast('chartype' as char(10)),\n"
          + " cast('varchartype' as varchar(20)),\n"
          + " cast('tinytexttype' as string),\n"
          + " cast('mediumtexttype' as string),\n"
          + " cast('texttype' as string),\n"
          + " cast('longtexttype' as string),\n"
          + " cast('binarytype' as bytes),\n"
          + " cast('varbinarytype' as bytes),\n"
          + " cast('tinyblobtype' as bytes),\n"
          + " cast('mediumblobtype' as bytes),\n"
          + " cast('blobtype' as bytes),\n"
          + " cast('longblobtype' as bytes),\n"
          + " cast(1.234 as float),\n"
          + " cast(2.456789 as double),\n"
          + " cast(123.456 as decimal(6,3)),\n"
          + " cast('2020-08-10' as date),\n"
          + " cast('15:30:29' as time),\n"
          + " cast('2020-08-10 15:30:29' as timestamp),\n"
          + " cast('2020-08-10 16:30:29' as timestamp),\n"
          + " cast(2020 as smallint),\n"
          + " true,\n"
          + " cast('{\"a\":1,\"b\":2}' as string),\n"
          + " cast('1' as string),\n"
          + " cast('a' as string)\n"
          + ")";
  
  public static String getInsertRowSql(String tableName, byte value1, short value2) {
    return format(INSERT_ROW_SQL_FORMAT, CATALOG_NAME, DATABASE_NAME, tableName, value1, value2);
  }
  
  public static String getCreateTableSql(String tableName) {
    return format(CREATE_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }
  
  public static String getDropTableSql(String tableName) {
    return format(DROP_TABLE_SQL_FORMAT, DATABASE_NAME, tableName);
  }
  
  public Row runByCatalogAndGetFirstRow(Map<String, String> properties,
                                        String executeSql,
                                        String tableName) throws Exception {
    // env
    TableEnvironment tableEnvironment = getTableEnvironment();
    // create test database and table
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    String dropTableSql = getDropTableSql(tableName);
    String createTableSql = getCreateTableSql(tableName);
    tiDBCatalog.sqlUpdate(CREATE_DATABASE_SQL, dropTableSql, createTableSql);
    // register catalog
    tableEnvironment.registerCatalog(CATALOG_NAME, tiDBCatalog);
    // insert data
    tableEnvironment.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 1));
    tableEnvironment.executeSql(getInsertRowSql(tableName, (byte) 2, (short) 2));
    // query
    TableResult tableResult = tableEnvironment.executeSql(executeSql);
    Row row = tableResult.collect().next();
    tiDBCatalog.sqlUpdate(dropTableSql);
    return row;
  }
  
  public void runByCatalog(Map<String, String> properties,
                           String tableName,
                           Consumer<TableEnvironment> customAction) {
    // create test database and table
    TiDBCatalog tiDBCatalog = new TiDBCatalog(properties);
    tiDBCatalog.open();
    String dropTableSql = getDropTableSql(tableName);
    String createTableSql = getCreateTableSql(tableName);
    tiDBCatalog.sqlUpdate(CREATE_DATABASE_SQL, dropTableSql, createTableSql);
    // table env
    TableEnvironment tEnv = getTableEnvironment();
    tEnv.registerCatalog(CATALOG_NAME, tiDBCatalog);
    // insert data
    tEnv.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 1));
    tEnv.executeSql(getInsertRowSql(tableName, (byte) 2, (short) 2));
    // custom action
    customAction.accept(tEnv);
    tiDBCatalog.sqlUpdate(dropTableSql);
  }
  
//  @RepeatedTest(10)
  @Test
  public void testBase() throws Exception {
    final String tableName = getRandomTableName();
    Row row = runByCatalogAndGetFirstRow(
        getDefaultProperties(),
        format("SELECT * FROM `%s`.`%s`.`%s`", CATALOG_NAME, DATABASE_NAME, tableName),
        tableName);
    assertNotNull(row);
  }
  
  @Test
  public void testReadLimit() throws Exception {
    final String tableName = getRandomTableName();
    final String selectSql = format("SELECT * FROM `%s`.`%s`.`%s` LIMIT 1",
        CATALOG_NAME, DATABASE_NAME, tableName);
    runByCatalog(
        getDefaultProperties(),
        tableName,
        tEnv -> {
          TableResult result = tEnv.executeSql(selectSql);
          int reCount = 0;
          for (CloseableIterator<Row> it = result.collect(); it.hasNext(); it.next()) {
            reCount++;
          }
          assertEquals(1, reCount);
        }
    );
  }
  
  @Test
  public void testReplicaRead() throws Exception {
    final String tableName = getRandomTableName();
    final String selectSQL = format("SELECT * FROM `%s`.`%s`.`%s` LIMIT 1",
        CATALOG_NAME, DATABASE_NAME, tableName);
    Row row1 = runByCatalogAndGetFirstRow(
        getDefaultProperties(),
        selectSQL,
        tableName);
    Row row2 = runByCatalogAndGetFirstRow(
        getDefaultProperties(p -> p.put(TIDB_REPLICA_READ, "true")),
        selectSQL,
        tableName);
    assertNotNull(row1);
    assertNotNull(row2);
    assertEquals(row1, row2);
  }
  
  @Test
  public void testUpsertAndRead() {
    final String tableName = getRandomTableName();
    final String selectSql = format("SELECT * FROM `%s`.`%s`.`%s` WHERE c1 = 1",
        CATALOG_NAME, DATABASE_NAME, tableName);
    runByCatalog(
        getDefaultProperties(p -> p.put(TIDB_WRITE_MODE, "upsert")),
        tableName,
        tEnv -> {
          tEnv.executeSql(getInsertRowSql(tableName, (byte) 1, (short) 114));
          Row row = tEnv.executeSql(selectSql).collect().next();
          assertNotNull(row);
          assertEquals((byte) 1, row.getField(0));
          assertEquals((short) 114, row.getField(1));
        });
  }
  
  @Test
  public void testFilterAndPushDown() {
    final String tableName = getRandomTableName();
    final String selectSql1 = format("SELECT * FROM `%s`.`%s`.`%s` LIMIT 1",
        CATALOG_NAME, DATABASE_NAME, tableName);
    final String selectSql2 =
        format("SELECT * FROM `%s`.`%s`.`%s` WHERE (c1 = 1 OR c3 = 1) AND c2 = 1",
            CATALOG_NAME, DATABASE_NAME, tableName);
    runByCatalog(
        getDefaultProperties(),
        tableName,
        tEnv -> {
          Row row1 = tEnv.executeSql(selectSql1).collect().next();
          Row row2 = tEnv.executeSql(selectSql2).collect().next();
          assertNotNull(row1);
          assertNotNull(row2);
          assertEquals(row1, row2);
        });
  }
  
  @Test
  public void testColumnPruner() {
    String tableName = getRandomTableName();
    String selectSql1 = format("SELECT * FROM `%s`.`%s`.`%s` LIMIT 1",
        CATALOG_NAME, DATABASE_NAME, tableName);
    // select 10 column randomly
    Random random = new Random();
    int[] ints = IntStream.range(0, 10).map(i -> random.nextInt(29)).toArray();
    String selectSql2 = format("SELECT %s FROM `%s`.`%s`.`%s` LIMIT 1",
        Arrays.stream(ints)
            .mapToObj(i -> "c" + (i + 1))
            .collect(Collectors.joining(",")),
        CATALOG_NAME, DATABASE_NAME, tableName);
    
    runByCatalog(
        getDefaultProperties(),
        tableName,
        tEnv -> {
          Row row1 = tEnv.executeSql(selectSql1).collect().next();
          Row row2 = tEnv.executeSql(selectSql2).collect().next();
          assertNotNull(row1);
          assertNotNull(row2);
          assertEquals(row2, copyRow(row1, ints));
        });
  }
  
  private Row copyRow(Row row, int[] indexes) {
    Row newRow = new Row(indexes.length);
    for (int i = 0; i < indexes.length; i++) {
      newRow.setField(i, row.getField(indexes[i]));
    }
    return newRow;
  }
  
  
  
}

