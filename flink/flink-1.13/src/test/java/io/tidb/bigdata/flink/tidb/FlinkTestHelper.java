package io.tidb.bigdata.flink.tidb;

import io.tidb.bigdata.tidb.ClientConfig;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class FlinkTestHelper {
  
  public static final String ARG_TIDB_HOST = "TIDB_HOST";
  
  public static final String ARG_TIDB_PORT = "TIDB_PORT";
  
  public static final String ARG_TIDB_USER = "TIDB_USER";
  
  public static final String ARG_TIDB_PASSWORD = "TIDB_PASSWORD";
  
  public static final String tidbHost = getEnvOrDefault(ARG_TIDB_HOST, "127.0.0.1");
  
  public static final String tidbPort = getEnvOrDefault(ARG_TIDB_PORT, "4000");
  
  public static final String tidbUser = getEnvOrDefault(ARG_TIDB_USER, "root");
  
  public static final String tidbPassword = getEnvOrDefault(ARG_TIDB_PASSWORD, "");
  
  public static final String CATALOG_NAME = "tidb";
  
  public static final String DATABASE_NAME = "test";
  
  
  private static String getEnvOrDefault(String key, String defaultVal) {
    String tmp = System.getenv(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }
    tmp = System.getProperty(key);
    if (tmp != null && !tmp.equals("")) {
      return tmp;
    }
    return defaultVal;
  }
  
  public static Map<String, String> getDefaultProperties() {
    Map<String, String> properties = new HashMap<>();
    properties.put(ClientConfig.DATABASE_URL, String.format(
        "jdbc:mysql://%s:%s/test?serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=CONVERT_TO_NULL&tinyInt1isBit=false",
        tidbHost,
        tidbPort)
    );
    properties.put(ClientConfig.USERNAME, tidbUser);
    properties.put(ClientConfig.PASSWORD, tidbPassword);
    properties.put(ClientConfig.MAX_POOL_SIZE, "1");
    properties.put(ClientConfig.MIN_IDLE_SIZE, "1");
    return properties;
  }
  
  public static Map<String, String> getDefaultProperties(Consumer<Map<String, String>> modifyFunc) {
    Map<String, String> properties = getDefaultProperties();
    modifyFunc.accept(properties);
    return properties;
  }
  
  public static String getRandomTableName() {
    return UUID.randomUUID().toString().replace("-", "_");
  }
  
  public static TableEnvironment getTableEnvironment() {
    return TableEnvironment.create(EnvironmentSettings
        .newInstance()
        .useBlinkPlanner()
        .inBatchMode()
        .build());
  }
  
}
