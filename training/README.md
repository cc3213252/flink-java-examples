# 官网练习

## 四个生产级的综合练习

https://github.com/apache/flink-training  

1、RideCountExample  
这个最基础，但是只有java版本  

2、RideCleansingSolution
数据清洗，过滤非纽约市的记录

3、HourlyTipsSolution  
https://github.com/apache/flink-training/tree/master/hourly-tips  
每小时最大费用，先统计每个司机每个小时赚到的小费，再求最大值  
窗口处理函数的使用  

4、RidesAndFaresSolution
富化信息，一个行驶流，一个车费流，通过rideId关联，富化后输出  
Stateful Enrichment的使用  

5、LongRidesSolution  
疲劳驾驶告警，驾驶超两小时告警  
getRuntimeContext、KeyedProcessFunction使用  

## 问题

connect不太理解，资料https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/learn-flink/etl.html