# Getting Started

### 目录

1. base
   1.1. 批处理进行单词计数 <br/>
   1.2. 流处理进行单词计数

2. env
   创建批处理、流处理、本地运行环境 

3. source
    3.1. 从集合中读取数据 <br/>
    3.2. 从文件中读取数据 <br/>
    3.3. 从kafka读取数据 <br/>
    
4. transform (转换算子) <br/>
    4.1. map <br/>
    4.2. flagmap <br/>
    4.3. filter <br/>
    4.4. keyBy <br/>
    4.5. map-reduce <br/>
    4.6. Split <br/>
    4.7. Select <br/>
    4.8. connect <br/>
    4.9. CoMap、CoFlatMap <br/>
    4.10. union <br/>

Connect与Union 区别： 
1. Union 之前两个流的类型必须是一样，Connect 可以不一样，在之后的 coMap中再去调整成为一样的 <br/>
2. Connect 只能操作两个流， Union 可以操作多个
