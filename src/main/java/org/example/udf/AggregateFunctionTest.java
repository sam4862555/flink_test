package org.example.udf;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class AggregateFunctionTest {

    /**
     * 1)For each set of rows that needs to be aggregated, the runtime will create an empty accumulator
     *     by calling createAccumulator().
     * 2)Subsequently, the accumulate(...) method of the function is called for each input row to update the accumulator.
     * 3)Once all rows have been processed, the getValue(...) method of the function is called to compute and
     *     return the final result.
     */
    /**  !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
     * aggregate functtion 必须要实现的方法
     * 1. createAccumulator() 创建累加器
     * 2. accumulate(...) 更新累加器
     * 3. getValue() 返回最终的统计结果
     * @param args
     */

    /** 其他的函数，如果在window 中许需要实现
     * The following methods of AggregateFunction are required depending on the use case:
     *
     * retract(...) is required for aggregations on OVER windows.
     * merge(...) is required for many bounded aggregations and session window and hop window aggregations. Besides, this method is also helpful for optimizations. For example, two phase aggregation optimization requires all the AggregateFunction support merge method.
     * If the aggregate function can only be applied in an OVER window, this can be declared by returning the requirement FunctionRequirement.OVER_WINDOW_ONLY in getRequirements().
     * @param args
     */
    public static class WeightAvgAccumulator{
        public long sum = 0;
        public int ct = 0;
    }

    public static class WeightAggFunction extends AggregateFunction<Long,WeightAvgAccumulator>{

        public WeightAvgAccumulator createAccumulator(){
            return new WeightAvgAccumulator();
        }

        public void accumulate(WeightAvgAccumulator acc, Long addValue, Integer weight){
            acc.sum += addValue * weight;
            acc.ct += weight;
        }

        @Override
        public Long getValue(WeightAvgAccumulator acc) {
            if (acc.ct == 0) {
                return null;
            } else {
                return acc.sum/acc.ct;
            }
        }
    }


    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.newInstance().inStreamingMode().build());

        tEnv.registerFunction("scalarTest", new ScalaFunctionTest.TestScalaFunction(" ---"));

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


    }
}
