import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

/**
 * @author Mika
 * @version 基于前缀的相似选择
 */
public class PrefixSelect {
    static double tau = 0.8;//阈值
    static int minPartitions=1;//分区数

    public static void main(String[] args) {
        String query;

        SparkConf conf;
        JavaSparkContext sc;
        String indexPath;
        if(Tool.local) {
            query="FFinding Discriminative Filters for Specific Degradations in Blind Super-Resolution";
            conf = new SparkConf()
                    .setAppName("Mika")
                    .setMaster("local"); // 集群环境则注释掉该行
            sc = new JavaSparkContext(conf);
            indexPath="file:///home/mika/Desktop/mika_java/mika-classes/prefix_index";
        }
        else{
            tau = Double.parseDouble(args[0]);
            minPartitions = Integer.parseInt(args[1]);
            query = args[2];//待查询字符串
            conf = new SparkConf()
                    .setAppName("Mika");
            sc = new JavaSparkContext(conf);
            indexPath = "hdfs://acer:9000/prefix_index";
        }

        long startTime = System.currentTimeMillis();//读完索引文件后，开始计时。事实上shell中每次查询都从这开始


        String cleanQuery = Tool.getCleanStr(query);
        JavaPairRDD<String, List<String>> resultTuples;
        JavaRDD<String> indexLines=sc.emptyRDD();//创建空的RDD


        System.out.println("开始比较前缀");
        List<String> querySigs = PrefixFilter.getPrefix(cleanQuery);//获得查询的前缀
        System.out.println("标签对应哈希值：");
        HashPartitioner hp=new HashPartitioner(minPartitions);
        if(Tool.local){
            indexLines = sc.textFile(indexPath);
        }
        else {
            JavaRDD<String> partitionIndexLines;
            for (String sig : querySigs) {
                int hashCode = hp.getPartition(sig);//该段的哈希值
                if (hashCode < 10) {
                    indexPath = "hdfs://acer:9000/prefix_index/part-0000" + hashCode;
                } else if (hashCode < 100) {
                    indexPath = "hdfs://acer:9000/prefix_index/part-000" + hashCode;
                } else {
                    indexPath = "hdfs://acer:9000/prefix_index/part-00" + hashCode;
                }
                partitionIndexLines = sc.textFile(indexPath);
                indexLines = indexLines.union(partitionIndexLines);
            }
        }

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
        resultTuples = sig2List.filter(//筛选前缀重叠的
                new Function<Tuple2<String, List<String>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, List<String>> tuple) throws Exception {
                        return querySigs.contains(tuple._1);
                    }
                }
        );

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

        //5.相似度验证,输出结果
        List<Tuple2<String, Integer>> results = uniqueRecords.collect();
        System.out.println("---------Results:----------");
        int cnt = 0;
        for (Tuple2<String, Integer> tuple : results) {
            double similarity = Tool.jaccardSimilarity(cleanQuery, tuple._1);
            if(similarity >= tau) {
                cnt += 1;
                System.out.printf("%d: %s: %f\n", cnt, tuple._1, similarity);
            }
        }

        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("--------总时间：%d 毫秒--------\n", usedTime);
    }
}
