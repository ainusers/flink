# Getting Started

### 目录

1. base
   1.1. 批处理进行单词计数
   1.2. 流处理进行单词计数

2. env
   创建批处理、流处理、本地运行环境

3. source
    3.1. 从集合中读取数据
    3.2. 从文件中读取数据
    3.3. 从kafka读取数据
    
4. transform (转换算子)
    4.1. map
    4.2. flagmap
    4.3. filter
    4.4. keyBy
    4.5. map-reduce
    4.6. Split
    4.7. Select
    4.8. connect
    4.9. CoMap、CoFlatMap
    4.10. union

Connect与Union 区别： 
1. Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap中再去调整成为一样的
2. Connect 只能操作两个流， Union 可以操作多个
