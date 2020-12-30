package windowing;

import org.apache.flink.api.common.eventtime.AscendingTimestampsWatermarks;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.delta.DeltaFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.DeltaTrigger;

import java.util.Arrays;
import java.util.Random;
/*
* 初始化速度是50千米/小时，转化成米/秒时要除以3.6
* 数据源每100毫秒输出一次，输出每辆车的id、速度、距离(米)、系统时间
* 随机地给每辆车加或减5千米/小时
*
* 每10秒清空数据
* 当有一辆车偏移50米时触发一次窗口结算
* 取速度最大值
* */
public class TopSpeedWindowing {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        @SuppressWarnings({"rawtypes", "serial"})
        DataStream<Tuple4<Integer, Integer, Double, Long>> carData = env.addSource(CarSource.create(2));
        DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData
                .assignTimestampsAndWatermarks(new CarTimestamp())
                .keyBy(value -> value.f0)
                .window(GlobalWindows.create())
                .evictor(TimeEvictor.of(Time.seconds(10)))
                .trigger(DeltaTrigger.of(50, new CustomDelta(), carData.getType().createSerializer(env.getConfig())))
                .maxBy(1);
        topSpeeds.print();
        env.execute();
    }

    private static class CarSource implements SourceFunction<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;
        private Integer[] speeds;
        private Double[] distances;

        private Random rand = new Random();

        private volatile boolean isRunning = true;

        private CarSource(int numOfCars) {
            speeds = new Integer[numOfCars];
            distances = new Double[numOfCars];
            Arrays.fill(speeds, 50);
            Arrays.fill(distances, 0d);
        }

        public static CarSource create(int cars) {
            return new CarSource(cars);
        }

        @Override
        public void run(SourceContext<Tuple4<Integer, Integer, Double, Long>> ctx) throws Exception {

            while (isRunning) {
                Thread.sleep(100);
                for (int carId = 0; carId < speeds.length; carId++) {
                    if (rand.nextBoolean()) {
                        speeds[carId] = Math.min(100, speeds[carId] + 5);
                    } else {
                        speeds[carId] = Math.max(0, speeds[carId] - 5);
                    }
                    distances[carId] += speeds[carId] / 3.6d;
                    Tuple4<Integer, Integer, Double, Long> record = new Tuple4<>(carId,
                            speeds[carId], distances[carId], System.currentTimeMillis());
                    ctx.collect(record);
                }
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    private static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
            return element.f3;
        }
    }

    private static class CustomDelta implements DeltaFunction<Tuple4<Integer, Integer, Double, Long>> {
        private static final long serialVersionUID = 1L;

        @Override
        public double getDelta(Tuple4<Integer, Integer, Double, Long> oldDataPoint, Tuple4<Integer, Integer, Double, Long> newDataPoint) {
            return newDataPoint.f2 - oldDataPoint.f2;
        }
    }
}
