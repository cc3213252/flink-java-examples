package iteration;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Random;
/*
1、随机生成100组，每组两个数据
2、每组复制一份，最后加一个标记第几组数据，这样每组就有5个数据了
4、最后输出的是过滤完了后的原始数据，加一个第几组数据标记

问题：
1、f2 + f3什么意思
2、这个功能和用过滤来解决区别是什么，和旁路输出相比呢
* */
public class IterateExample {
    private static final int BOUND = 100;
    private static final OutputTag<Tuple5<Integer, Integer, Integer, Integer, Integer>> ITERATE_TAG =
            new OutputTag<Tuple5<Integer, Integer, Integer, Integer, Integer>>("iterate") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setBufferTimeout(1);
        DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new RandomFibonacciSource());
        IterativeStream<Tuple5<Integer, Integer, Integer, Integer, Integer>> it = inputStream
                .map(new InputMap())
                .iterate(5000L);
        SingleOutputStreamOperator<Tuple5<Integer, Integer, Integer, Integer, Integer>> step = it.process(new Step());
        it.closeWith(step.getSideOutput(ITERATE_TAG));
        step.map(new OutputMap()).print();
        env.execute();
    }

    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        private Random rnd = new Random();

        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {

            while (isRunning && counter < BOUND) {
                int first = rnd.nextInt(BOUND / 2 - 1) + 1;
                int second = rnd.nextInt(BOUND / 2 - 1) + 1;
                ctx.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class InputMap implements MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer,
            Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws
                Exception {
            return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
        }
    }

    public static class Step
            extends ProcessFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public void processElement(
                Tuple5<Integer, Integer, Integer, Integer, Integer> value,
                Context ctx,
                Collector<Tuple5<Integer, Integer, Integer, Integer, Integer>> out) throws Exception {
            Tuple5<Integer, Integer, Integer, Integer, Integer> element = new Tuple5<>(
                    value.f0,
                    value.f1,
                    value.f3,
                    value.f2 + value.f3,
                    ++value.f4);

            if (value.f2 < BOUND && value.f3 < BOUND) {
                ctx.output(ITERATE_TAG, element);
            } else {
                out.collect(element);
            }
        }
    }

    public static class OutputMap implements MapFunction<Tuple5<Integer, Integer, Integer, Integer, Integer>,
            Tuple2<Tuple2<Integer, Integer>, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Tuple2<Integer, Integer>, Integer> map(Tuple5<Integer, Integer, Integer, Integer, Integer>
                                                                     value) throws
                Exception {
            return new Tuple2<>(new Tuple2<>(value.f0, value.f1), value.f4);
        }
    }
}
