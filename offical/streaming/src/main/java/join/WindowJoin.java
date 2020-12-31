package join;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import join.WindowJoinSampleData.*;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/*
* 把姓名、工资流和姓名、等级流合并
* 1秒产生3个元素
* */
public class WindowJoin {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple2<String, Integer>> grades = GradeSource
                .getSource(env, 3L)
                .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Tuple2<String, Integer>> salaries = SalarySource
                .getSource(env, 3L)
                .assignTimestampsAndWatermarks(IngestionTimeWatermarkStrategy.create());

        DataStream<Tuple3<String, Integer, Integer>> joinedStream = runWindowJoin(grades, salaries, 2000);
        joinedStream.print().setParallelism(1);
        env.execute();
    }

    private static class IngestionTimeWatermarkStrategy<T> implements WatermarkStrategy<T> {
        private IngestionTimeWatermarkStrategy(){}

        public static <T> IngestionTimeWatermarkStrategy<T> create() {
            return new IngestionTimeWatermarkStrategy<>();
        }

        @Override
        public WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new AscendingTimestampsWatermarks<>();
        }

        @Override
        public TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (event, timestamp) -> System.currentTimeMillis();
        }
    }

    public static DataStream<Tuple3<String, Integer, Integer>> runWindowJoin(
            DataStream<Tuple2<String, Integer>> grades,
            DataStream<Tuple2<String, Integer>> salaries,
            long windowSize) {
        return grades.join(salaries)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.milliseconds(2000)))
                .apply(new CustomJoin());
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }

    private static class CustomJoin implements JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>,
            Tuple3<String, Integer, Integer>> {
        @Override
        public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) {
            return new Tuple3<String, Integer, Integer>(first.f0, first.f1, second.f1);
        }
    }
}
