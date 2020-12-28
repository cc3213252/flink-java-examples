package windowing;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import wordcount.WordCount3;
import wordcount.util.WordCountData;

/*
* 每5个数统计一次，统计的是最近10个数
* */
public class WindowWordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.fromElements(WordCountData.WORDS);
        text.flatMap(new WordCount3.Tokenizer())
                .keyBy(0)
                .countWindow(10, 5)
                .sum(1)
                .print();

        env.execute();
    }
}
