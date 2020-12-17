## DataStream Api之side Outputs

https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/side_output.html

## 碰到问题

Exception in thread "main" org.apache.flink.runtime.client.JobExecutionException: Job execution failed.

【ok】提示@Override用的这个方法中父类没有这个方法
SideOutputExample这个程序从源码中拷贝过来时，processElement这个函数中Context ctx这个参数类型会变成ProcessFunction.Context

