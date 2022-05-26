import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * @author Mika
 * @version 生成传统索引
 */
public class TraditionalFilter {
    //相似度阈值
    static double tau=0.6;
    static int minPartitions=1;//分区数

    static Set<String>stopWords=new HashSet<>();
    public static void main(String[] args) {

        SparkConf conf;
        JavaSparkContext sc;
        JavaRDD<String> lines;
        if(Tool.local) {
            conf=new SparkConf()
                    .setAppName("Mika")
                    .setMaster("local");
            sc=new JavaSparkContext(conf);
            lines = sc.textFile("data.txt");
        }
        else{
            tau=Double.parseDouble(args[0]);
            minPartitions=Integer.parseInt(args[1]);
            conf=new SparkConf()
                    .setAppName("Mika");
            sc=new JavaSparkContext(conf);
            lines = sc.textFile("hdfs://acer:9000/data", minPartitions);
        }

        //设置停用词
        Collections.addAll(stopWords,"a","an","the","in","on","at","for","with","to","from");
        long startTime = System.currentTimeMillis();

        //1.得到（元素，记录）对
        JavaRDD<Tuple2<String,String>> element2Record=lines.flatMap(
                new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(String line) throws Exception {
                        String[] elements=Tool.getCleanStr(line).split(" ");//获取前缀
                        List<Tuple2<String,String>>res=new ArrayList<>();
                        for(String sig:elements){
                            res.add(new Tuple2<>(sig,line));
                        }
                        return res.iterator();
                    }
                }
        );

        //RDD2.合并所有相同标签的倒排列表
        JavaPairRDD<String, Iterable<String>> prefix2InvertedList=element2Record.mapToPair(
                new PairFunction<Tuple2<String,String>,String,String>(){
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String,String> call(Tuple2<String,String> t){
                        return new Tuple2<>(t._1, t._2);
                    }
                }).groupByKey().partitionBy(new HashPartitioner(minPartitions));//相同key的value合并
        // 并使用哈希分区，这样哈希值为i的索引保存到索引文件part-i中，相似选择时只需查该文件即可

        //将RDD保存到index目录
        String outputPath;
        if(Tool.local){
            outputPath="traditional_index";
        }
        else{
            outputPath="hdfs://acer:9000/traditional_index";
        }
        prefix2InvertedList.saveAsTextFile(outputPath);


        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("--------总时间：%d 毫秒--------\n", usedTime);
    }
}
