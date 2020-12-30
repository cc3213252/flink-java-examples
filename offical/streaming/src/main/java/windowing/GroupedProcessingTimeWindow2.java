package windowing;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*
这个例子和上一个的区别是，上一个reduce随时进行聚合，这个是窗口被触发的时候一起聚合
* */
public class GroupedProcessingTimeWindow2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStream<Tuple2<Long, Long>> stream = env.addSource(new GroupedProcessingTimeWindow.DataSource());
        stream.keyBy(new FirstFieldKeyExtractor<Tuple2<Long, Long>, Long>())
                .window(SlidingProcessingTimeWindows.of(Time.milliseconds(2500), Time.milliseconds(500)));
//                .apply(new SummingWindowFunction())
//                .print();
        env.execute();
    }

    private static class FirstFieldKeyExtractor<Type extends Tuple, Key> implements KeySelector<Type, Key> {
        @Override
        @SuppressWarnings("unchecked")
        public Key getKey(Type value) {
            return (Key) value.getField(0);
        }
    }

    public static class SummingWindowFunction implements WindowFunction<Tuple2<Long, Long>, Tuple2<Long, Long>, Long, Window> {
        @Override
        public void apply(Long key, Window window, Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long>> out) {
            long sum = 0;
            for (Tuple2<Long, Long> value: values) {
                sum += value.f1;
            }
            out.collect(new Tuple2<>(key, sum));
        }
    }
}
