package org.example;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

public class FlinkSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                int subtask = this.getRuntimeContext().getIndexOfThisSubtask();
                int parallelism = this.getRuntimeContext().getNumberOfParallelSubtasks();
                System.out.println(parallelism);
                for(int i= 0; i<10; i++){
                    if(i % parallelism == subtask){
                        sourceContext.collect(i);
                    }
                }
            }

            @Override
            public void cancel() {

            }
        }).print();

        env.execute();
    }
}
