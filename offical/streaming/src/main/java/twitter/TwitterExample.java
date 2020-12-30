package twitter;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import twitter.util.TwitterExampleData;

import java.util.StringTokenizer;

/*
text字段如果有\t\n\r\f等，则转成列表输出，输出时去空格转小写
主要演示了如何解析json字段
TwitterSource类演示了如何从外部服务直接接入数据
* */
public class TwitterExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> streamSource = env.fromElements(TwitterExampleData.TEXTS);
        DataStream<Tuple2<String, Integer>> tweets = streamSource
                .flatMap(new SelectEnglishAndTokenizerFlatMap())
                .keyBy(value -> value.f0)
                .sum(1);
        tweets.print();
        env.execute();
    }

    public static class SelectEnglishAndTokenizerFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;
        private transient ObjectMapper jsonParser;

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            if (jsonParser == null) {
                jsonParser = new ObjectMapper();
            }
            JsonNode jsonNode = jsonParser.readValue(value, JsonNode.class);
            boolean isEnglish = jsonNode.has("user") && jsonNode.get("user").has("lang") && jsonNode.get("user").get("lang").asText().equals("en");
            boolean hasText = jsonNode.has("text");
            if (isEnglish && hasText) {
                StringTokenizer tokenizer = new StringTokenizer(jsonNode.get("text").asText());

                while (tokenizer.hasMoreTokens()) {
                    String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

                    if (!result.equals("")) {
                        out.collect(new Tuple2<>(result, 1));
                    }
                }
            }
        }
    }
}
