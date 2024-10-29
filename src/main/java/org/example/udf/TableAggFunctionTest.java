package org.example.udf;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class TableAggFunctionTest {

    // mutable accumulator of structured type for the aggregate function
    public static class Top2Accumulator {
        public Integer first;
        public Integer second;
    }

    // function that takes (value INT), stores intermediate results in a structured
// type of Top2Accumulator, and returns the result as a structured type of Tuple2<Integer, Integer>
// for value and rank
    public static class Top2 extends TableAggregateFunction<Integer, Top2Accumulator> {

        @Override
        public Top2Accumulator createAccumulator() {
            Top2Accumulator acc = new Top2Accumulator();
            acc.first = Integer.MIN_VALUE;
            acc.second = Integer.MIN_VALUE;
            return acc;
        }

        public void accumulate(Top2Accumulator acc, Integer value) {
            if (value > acc.first) {
                acc.second = acc.first;
                acc.first = value;
            } else if (value > acc.second) {
                acc.second = value;
            }
        }

        public void merge(Top2Accumulator acc, Iterable<Top2Accumulator> it) {
            for (Top2Accumulator otherAcc : it) {
                accumulate(acc, otherAcc.first);
                accumulate(acc, otherAcc.second);
            }
        }

        public void emitValue(Top2Accumulator acc, Collector<Integer> out) {
            // emit the value and rank
            if (acc.first != Integer.MIN_VALUE) {
                out.collect(acc.first);
            }
            if (acc.second != Integer.MIN_VALUE) {
                out.collect(acc.second);
            }
        }
    }
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.createTemporarySystemFunction("maxTop2Rows",TableAggFunctionTest.Top2.class);

        tEnv.executeSql("create table orders(" +
                "user_id int," +
                "score int," +
                "order_time timestamp" +
                ") " +
                "with (" +
                "   'connector' = 'datagen'" + "," +
                "   'rows-per-second' = '1'" + "," +
                "   'fields.user_id.min' = '1'" + "," +
                "   'fields.user_id.max' = '5'" + "," +
                "   'fields.score.min' = '1'" + "," +
                "   'fields.score.max' = '10'" +
                ")"
        );

        tEnv.executeSql("select user_id, maxTop2Rows(score) from orders group by user_id").print();

    }
}
