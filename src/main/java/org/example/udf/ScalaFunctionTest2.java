package org.example.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;

import java.util.Optional;

public class ScalaFunctionTest2 {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.createTemporarySystemFunction("CalLength", TestScalaFunction2.class);

        tEnv.executeSql("create table orders(" +
                "order_id bigint," +
                "price decimal(10,2)," +
                "order_time timestamp" +
                ") " +
                "with (" +
                "   'connector' = 'datagen'" + "," +
                "   'rows-per-second' = '1'" + "," +
                "   'fields.order_id.min' = '1'" + "," +
                "   'fields.order_id.max' = '1000'" + "," +
                "   'fields.price.min' = '1.0'" + "," +
                "   'fields.price.max' = '1000.0'" +
                ")"
        );

        TableResult resTable = tEnv.executeSql("" +
                "select " +
                "   order_id " + "," +
                "   price " + "," +
                "   CalLength(cast(order_id as string)) as val_a " + "," +
                "   CalLength(cast(price as string)) as val_b " +
                "from orders");
        resTable.print();
    }

    public static class TestScalaFunction2 extends ScalarFunction {

        public Integer eval(String s) {
            return s.length();
        }
    }
}

