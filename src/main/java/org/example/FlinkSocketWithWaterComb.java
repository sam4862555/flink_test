package org.example;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class FlinkSocketWithWaterComb {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> src = env.socketTextStream("localhost", 9090);
        SingleOutputStreamOperator<Tuple2<String, Integer>> withWaterMark = src
                .map(new StringTuple2RichMapFunction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.of(5, TimeUnit.SECONDS)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> stringIntegerTuple2) {
                        return stringIntegerTuple2.f1 * 1000L;
                    }
                })
        ;

        DataStream<String> src2 = env.socketTextStream("localhost", 9091);
        SingleOutputStreamOperator<Tuple2<String, Integer>> withWaterMark2= src2
                .map(new StringTuple2RichMapFunction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.of(5, TimeUnit.SECONDS)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> stringIntegerTuple2) {
                        return stringIntegerTuple2.f1 * 1000L;
                    }
                });

        withWaterMark.union(withWaterMark2).process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            @Override
            public void processElement(Tuple2<String, Integer> stringIntegerTuple2, ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("current watermark : " + context.timerService().currentWatermark());
                collector.collect(stringIntegerTuple2);
            }
        }).print();



        env.execute();
    }

    private static class StringTuple2RichMapFunction extends RichMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {

            return new Tuple2<>(s.split(",")[0], Integer.parseInt(s.split(",")[1]));
        }
    }
}
