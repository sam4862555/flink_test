package org.example.sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;

public class DataGenTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.executeSql("create table orders(" +
                "order_id bigint," +
                "price decimal(10,2)," +
                "order_time timestamp" +
                ") " +
                "with (" +
                "   'connector' = 'datagen'" + "," +
                "   'rows-per-second' = '1'" + "," +
                "   'fields.order_id.min' = '100'" + "," +
                "   'fields.order_id.max' = '105'" +  "," +
                "   'fields.price.min' = '0.0'" +  "," +
                "   'fields.price.max' = '1000.0'" +
                ")"
        );

        Table resTable = tEnv.sqlQuery("select * from orders");
        DataStream<Row> resStream = tEnv.toChangelogStream(resTable, Schema.newBuilder().build(), ChangelogMode.insertOnly());
        resStream.print();

        env.execute("Orders Data Generation and Query");
    }
}
