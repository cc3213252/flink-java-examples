# flink框架java版入门程序

最佳flink-java工程创建实践

## 工程创建

1、创建主工程，用Maven创建    

2、右键创建maven子工程

3、在另外单独的目录执行

```bash
mvn archetype:generate                               \
  -DarchetypeGroupId=org.apache.flink              \
  -DarchetypeArtifactId=flink-quickstart-java      \
  -DarchetypeVersion=1.12.0
```
4、把需要的pom文件内容拷贝到创建的maven工程来

5、在Run/Debug Configuation里面，把Include Dependencies with Provided scope打上勾