package org.example.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ScalarFunctionTestSocketNotImpl {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        // 创建表来表示 socket source
        tEnv.executeSql("CREATE TABLE socket_table (\n" +
                "  data STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'socket',\n" +
                "  'hostname' = 'localhost',\n" +
                "  'port' = '9999'\n" +
                ")");

        // 创建表来表示 print sink
        tEnv.executeSql("CREATE TABLE print_table (\n" +
                "  data STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'print'\n" +
                ")");

        // 从 socket source 读取数据并写入到 print sink
        tEnv.executeSql("INSERT INTO print_table SELECT data FROM socket_table");
    }
}
