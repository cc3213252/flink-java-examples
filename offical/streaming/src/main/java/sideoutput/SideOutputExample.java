package sideoutput;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import wordcount.util.WordCountData;

/*
* IngestionTime 数据进入flink的时间，无法处理任何无序事件或延迟数据，具有自动分配时间戳和自动生成水印功能
*
* 长度大于5的拒绝掉，并实时打印拒绝的数，后按事件时间5秒一次处理统计数据
* */
public class SideOutputExample {
    private static final OutputTag<String> rejectedWordsTag = new OutputTag<String>("rejected"){};

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements(WordCountData.WORDS);
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        SingleOutputStreamOperator<Tuple2<String, Integer>> tokenized = text
                .keyBy(new CustomKeySelector())
                .process(new Tokenizer());

        DataStream<String> rejectedWords = tokenized
                .getSideOutput(rejectedWordsTag)
                .map(new CustomMap());

        DataStream<Tuple2<String, Integer>> counts = tokenized
                .keyBy(value -> value.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sum(1);
        counts.print();
        rejectedWords.print();
        env.execute();
    }

    private static class CustomKeySelector implements KeySelector<String, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public Integer getKey(String value) throws Exception {
            return 0;
        }
    }

    public static final class Tokenizer extends ProcessFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");

            for (String token: tokens) {
                if (token.length() > 5) {
                    ctx.output(rejectedWordsTag, token);
                }else if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    private static class CustomMap implements MapFunction<String, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map(String value) throws Exception {
            return "rejected:" + value;
        }
    }
}
