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






public class FlinkSocketWithWaterSplit {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(200);

        DataStream<String> src = env.socketTextStream("localhost", 9090);
        DataStream<Tuple2<String, Integer>> withWaterMark = src
                .map(new StringTuple2RichMapFunction())
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple2<String, Integer>>(Time.of(0, TimeUnit.SECONDS)) {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> stringIntegerTuple2) {
                        return stringIntegerTuple2.f1 * 1000L;
                    }
                });
//                .setParallelism(4)
//                .broadcast()
//                .keyBy(r -> r.f0)
////                .rebalance()
//                ;

        withWaterMark.process(new ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            @Override
            public void processElement(Tuple2<String, Integer> stringIntegerTuple2, ProcessFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>.Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
                System.out.println(
                        "index : " + this.getRuntimeContext().getIndexOfThisSubtask() + " , " +
                        "current watermark : " + context.timerService().currentWatermark());
                collector.collect(stringIntegerTuple2);
            }
        }).setParallelism(4).rebalance().print();

        env.execute();
    }

    private static class StringTuple2RichMapFunction extends RichMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {

            return new Tuple2<>(s.split(",")[0], Integer.parseInt(s.split(",")[1]));
        }
    }
}
