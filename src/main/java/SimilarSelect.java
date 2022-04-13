import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * @author Mika
 * @version 相似选择：给定查询q，返回倒排列表
 */

//过滤算法
enum Filter {
    Prefix, Segment
}

public class SimilarSelect {
    static double tau = 0.6;
    static Filter filter = Filter.Segment;//修改这里，选择过滤算法

    //判断前缀是否重叠
    static boolean isOverlapped(String s1, String s2) {
        List<String> list1 = new ArrayList<>(Arrays.asList(s1.split(" ")));
        List<String> list2 = new ArrayList<>(Arrays.asList(s2.split(" ")));
        return Tool.intersectionSize(list1, list2) > 0;
    }

    public static void main(String[] args) {
        int minPartitions=128;
        SparkConf conf = new SparkConf()
                .setAppName("Mika")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String indexPath;
        switch (filter) {
            case Prefix:
                indexPath = "./prefix_index/part-00000";
                break;
            case Segment:
                indexPath = "./segment_index/part-00000";
                break;
            default:
                return;
        }

        String query = "Finding Discriminative Filters for Specific Degradations in Blind Super-Resolution";//待查询字符串

        long startTime = System.currentTimeMillis();//读完索引文件后，开始计时。事实上shell中每次查询都从这开始

        //过滤，获得倒排列表集，有前缀和片段两种算法
        String cleanQuery = Tool.getCleanStr(query);
        JavaPairRDD<String, List<String>> resultTuples;
        JavaRDD<String> indexLines=sc.emptyRDD();//创建空的RDD
        switch (filter) {
            //前缀过滤:比较前缀看是否重叠
            case Prefix:
                indexLines = sc.textFile(indexPath,minPartitions);//读取索引文件，格式为(标签,[记录1,记录2])
                //切分索引表项，得到（标签，倒排列表）元组对
                JavaPairRDD<String, List<String>> sig2List = indexLines.mapToPair(
                        new PairFunction<String, String, List<String>>() {
                            //对于每一行
                            @Override
                            public Tuple2<String, List<String>> call(String line) throws Exception {
                                return Tool.getTuple(line);
                            }
                        }
                );

                System.out.println("开始比较前缀");
                String queryPrefix = PrefixFilter.getPrefix(cleanQuery);//获得查询的前缀
                resultTuples = sig2List.filter(//筛选前缀重叠的
                        new Function<Tuple2<String, List<String>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<String, List<String>> tuple) throws Exception {
                                //前缀是否重叠
                                return isOverlapped(queryPrefix, tuple._1);
                            }
                        }
                );
                break;
            //分段过滤:比较片段看是否相同
            case Segment:
                System.out.println("开始比较片段");
                String[] tokens = Tool.getCleanStr(cleanQuery).split(" ");//分词
                List<String> querySegments = SegmentFilter.getSegment(tokens, SegmentFilter.getSegmentMethod);

                assert querySegments != null;
                System.out.println("片段对应哈希值：");
                HashPartitioner hp=new HashPartitioner(minPartitions);
                JavaRDD<String>partitionIndexLines;
                for (String seg : querySegments) {
                    int hashCode=hp.getPartition(seg);//该段的哈希值
                    if(hashCode<100){
                        indexPath = "hdfs://acer:9000/segment_index/part-000"+hashCode;
                    }
                    else{
                        indexPath = "hdfs://acer:9000/segment_index/part-00"+hashCode;
                    }
                    partitionIndexLines = sc.textFile(indexPath);
                    indexLines=indexLines.union(partitionIndexLines);
                }
                //切分索引表项，得到（标签，倒排列表）元组对
                sig2List = indexLines.mapToPair(
                        new PairFunction<String, String, List<String>>() {
                            //对于每一行
                            @Override
                            public Tuple2<String, List<String>> call(String line) throws Exception {
                                return Tool.getTuple(line);
                            }
                        }
                );
                resultTuples = sig2List.filter(
                        new Function<Tuple2<String, List<String>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<String, List<String>> tuple) throws Exception {
                                return querySegments.contains(tuple._1);
                            }
                        }
                );
                break;
            default:
                resultTuples = null;
        }

        //3.将候选的倒排列表中所有记录编号加入集合
        JavaRDD<String> filteredRecords = resultTuples.flatMap(
                new FlatMapFunction<Tuple2<String, List<String>>, String>() {
                    @Override
                    public Iterator<String> call(Tuple2<String, List<String>> tuple) throws Exception {
                        return tuple._2.iterator();
                    }
                }
        );
        //4.去重
        JavaPairRDD<String, Integer> uniqueRecords = filteredRecords.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<>(s, 1);
                    }
                }
        ).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return 1;
                    }
                }
        );

        //5.相似度验证
        JavaPairRDD<String, Integer> resultRecords = uniqueRecords.filter(
                new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> tuple) throws Exception {
                        return Tool.jaccardSimilarity(cleanQuery, tuple._1) >= tau;
                    }
                }
        );
        //6.输出结果
        List<Tuple2<String, Integer>> results = resultRecords.collect();
        System.out.println("Results:");
        for (Tuple2<String, Integer> res : results) {
            System.out.println(res._1);
        }

        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("总时间：%d 毫秒", usedTime);
    }
}