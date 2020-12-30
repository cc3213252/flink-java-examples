## 相关资料

https://ci.apache.org/projects/flink/flink-docs-release-1.12/zh/dev/stream/operators/windows.html  
看到globalWindow可做windowWordCount、SessionWindowing    
看到WindowFunction可做GroupedProcessingTimeWindowExample  


## 练习顺序

WindowWordCount  
SessionWindowing  
GroupedProcessingTimeWindow  
TopSpeedWindowing  

## 问题

countWindow运行时有6个线程，输出结果不好理解
线程数是本机cpu核数 
  
sessionWindow不太理解  
GroupedProcessingTimeWindow2例子有编译错误  