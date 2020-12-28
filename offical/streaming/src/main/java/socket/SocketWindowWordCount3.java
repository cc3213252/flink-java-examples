package socket;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCount3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 11111, "\n");
        text.flatMap(new CustomFlatMap())
                .keyBy(value -> value.word)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce(new CustomReduce())
                .print();
        env.execute();
    }

    public static class WordWithCount {
        public String word;
        public long count;
        public WordWithCount(){}
        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }
        @Override
        public String toString() {
            return this.word + " : " + this.count;
        }
    }

    public static final class CustomFlatMap implements FlatMapFunction<String, WordWithCount> {
        @Override
        public void flatMap(String value, Collector<WordWithCount> out) {
            for (String word: value.split(" ")) {
                out.collect(new WordWithCount(word, 1L));
            }
        }
    }

    public static final class CustomReduce implements ReduceFunction<WordWithCount> {
        @Override
        public WordWithCount reduce(WordWithCount a, WordWithCount b) {
            return new WordWithCount(a.word, a.count + b.count);
        }
    }
}
