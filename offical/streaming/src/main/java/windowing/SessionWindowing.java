package windowing;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SessionWindowing {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();
        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));
        input.add(new Tuple3<>("a", 10L, 1));
        input.add(new Tuple3<>("c", 11L, 1));

        DataStream<Tuple3<String, Long, Integer>> source = env
                .addSource(new CustomSource(input));
        source.keyBy(value -> value.f0)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(3L)))
                .sum(2)
                .print();
        env.execute();
    }

    public static class CustomSource implements SourceFunction<Tuple3<String, Long, Integer>> {
        private static final long serialVersionUID = 1L;
        private List<Tuple3<String, Long, Integer>> input;

        public CustomSource(List<Tuple3<String, Long, Integer>> input) {
            this.input = input;
        }
        @Override
        public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
            for (Tuple3<String, Long, Integer> value: this.input) {
                ctx.collectWithTimestamp(value, value.f1);
                ctx.emitWatermark(new Watermark(value.f1 - 1));
            }
            ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
        }

        @Override
        public void cancel(){}
    }
}
