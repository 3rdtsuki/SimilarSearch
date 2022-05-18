#### 说明

实验基于Spark框架，`src/java`为代码目录

- PrefixFilter.java：前缀过滤算法生成倒排索引

- SegmentFilter.java：片段过滤算法生成倒排索引

- SimilarSelect.java：相似选择

- SimilarJoin.java：相似连接

- Tool.java：工具函数，包含数据处理和相似度计算

- BruteForceSelect.java：暴力的相似选择

Tool.java中控制本地还是集群运行

#### 运行方法

1.首先要把整个项目打成jar包

- IDEA中File->Project Structure->Project Settings->Artifacts->+->选定要打包的class。这里注意只保留第一项和最后一项。参考[Spark打包WordCount程序的jar包_sa726663676的博客-CSDN博客_wordcount打包](https://blog.csdn.net/sa726663676/article/details/120122230)

- 每次修改代码后，重新构建：Build->Build Artifacts->Build

2.启动Hadoop和Spark服务

3.在shell中运行（--class后面是要运行的java类，并且该类包含main方法，这里运行`SimilarSelect.java`）

```sh
# 本地环境
spark-submit \
--class SimilarSelect \
--master local[2] \
/home/mika/Desktop/mika_java/mika-classes/out/artifacts/mika_classes_jar/mika-classes.jar

# 集群环境
spark-submit \
--class SimilarSelect \
--master spark://acer:7077 \
/home/mika/Desktop/mika_java/mika-classes/out/artifacts/mika_classes_jar/mika-classes.jar
```

