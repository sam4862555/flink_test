package org.example.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class DataGenTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql("create table orders(" +
                "order_id bigint," +
                "price decimal(10,2)," +
                "order_time timestamp" +
                ") " +
                "with (" +
                "   'connector' = 'datagen'," +
                "   'rows-per-second' = '1'" +
                ")"
        );

        TableResult resTable = tEnv.executeSql("select * from orders");
        resTable.print();
    }
}
