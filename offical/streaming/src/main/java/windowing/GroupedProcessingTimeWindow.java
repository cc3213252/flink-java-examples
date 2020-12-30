package windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
不断产生一万以内的数据，每0.5秒滑动一次，统计2.5秒的数据，0.5秒内7000多的数据大概会出现270多次，随后次数越来越多，最多到367次时
2千万数据跑完停止
*/
public class GroupedProcessingTimeWindow {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<Tuple2<Long, Long>> stream = env.addSource(new DataSource());
        stream.keyBy(value -> value.f0)
                .window(SlidingProcessingTimeWindows.of(Time.milliseconds(2500), Time.milliseconds(500)))
                .reduce(new SummingReducer())
                .print();
        env.execute();
    }

    public static class DataSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
        private volatile boolean running = true;

        @Override
        public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
            final long startTime = System.currentTimeMillis();

            final long numElements = 20000000;
            final long numKeys = 10000;
            long val = 1L;
            long count = 0L;

            while (running && count < numElements) {
                count++;
                ctx.collect(new Tuple2<>(val++, 1L));

                if (val > numKeys) {
                    val = 1L;
                }
            }

            final long endTime = System.currentTimeMillis();
            System.out.println("Took " + (endTime - startTime) + " msecs for " + numElements + " values");
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    public static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
            return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
        }
    }
}
