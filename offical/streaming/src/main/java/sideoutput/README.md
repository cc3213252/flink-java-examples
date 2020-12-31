## 资料

https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/side_output.html  
https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/event_timestamps_watermarks.html

## 旁路输出

首先需要定义标示旁路输出流的OutputTag  
其次定义一个SingleOutputStreamOperator流，标记outputtag  
取出旁路结果getSideOutput  

## SingleOutputStreamOperator

从源码可知，是DataStream的子类，加入了用户定制化内容，因需要区分是源数据还是旁路数据，故旁路输出时用这个  

## SingleOutputStreamOperator例子

https://www.programcreek.com/java-api-examples/?api=org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator