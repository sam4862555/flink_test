package org.example.udf;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class TableFunctionTest {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.createTemporarySystemFunction("SplitTime", TableFunctionTest.SplitFunction.class);

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

        //todo : 几种写法都实验一下
//        //方式一: left join , 重新命名
//        TableResult resTable = tEnv.executeSql("" +
//                "select " +
//                "   order_id " + "," +
//                "   price " + "," +
//                "   day_str " + "," +
//                "   time_str " +
//                "from orders " +
//                "left join lateral table(SplitTime(cast(order_time as string))) AS T(day_str, time_str) on true");
//        resTable.print();

        //方式二: lateral table , 重新命名
        TableResult resTable = tEnv.executeSql("" +
                "select " +
                "   order_id " + "," +
                "   price " + "," +
                "   day_str " + "," +
                "   time_str " +
                "from orders, lateral table(SplitTime(cast(order_time as string))) AS T(day_str, time_str)");
        resTable.print();

    }



    @FunctionHint(output = @DataTypeHint("ROW<day_info STRING, time_info String>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            collect(Row.of(str.split(" ")[0], str.split(" ")[1]));
        }
    }
}
