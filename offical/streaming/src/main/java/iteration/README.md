## 应用场景

在流中创建“反馈（feedback）”循环，通过将一个算子的输出重定向到某个先前的算子。这对于定义不断更新模型的算法特别有用

迭代的数据流向：DataStream → IterativeStream → DataStream

## Fibonacci sequence

0、1、1、2、3、5、8、13， 即前两个数相加等于第三个数