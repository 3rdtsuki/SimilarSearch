import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

/**
 * @author Mika
 * @version 生成片段索引
 */
public class SegmentFilter {
    static double tau = 0.6;//相似度阈值
    static int minPartitions=1;//分区数

    static Set<String>stopWords=new HashSet<>();
    //平均分段法
    static List<String> getSegment(String[] tokens) {
        int n = tokens.length;
        int segmentNum = (int) Math.ceil((1 - tau) / tau * n) + 1;//段数
        float temp = (float) n / segmentNum;
        int k = n - (int) Math.floor(temp) * segmentNum;
        int frontLen = (int) Math.ceil(temp);//前k段长度
        int backLen = (int) Math.floor(temp);//剩余段长度
        int[] index2Len = new int[segmentNum];
        for (int i = 0; i < k; ++i) {
            index2Len[i] = frontLen;
        }
        for (int i = k; i < segmentNum; ++i) {
            index2Len[i] = backLen;
        }

        List<String> segments = new ArrayList<>();
        int cnt = 0;
        for (int i = 0; i < segmentNum; ++i) {
            StringBuilder sb = new StringBuilder();
            sb.append(tokens[cnt++]);
            for (int j = 1; j < index2Len[i]; ++j) {
                sb.append(" ");
                sb.append(tokens[cnt++]);
            }
            String s=sb.toString();
            if(stopWords.contains(s))continue;
            segments.add(s);
        }
        return segments;
    }

    public static void main(String[] args) throws IOException {

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
            tau=Double.parseDouble(args[0]);//阈值
            minPartitions=Integer.parseInt(args[1]);//分区数
            conf=new SparkConf()
                    .setAppName("Mika");
            sc=new JavaSparkContext(conf);
            lines = sc.textFile("hdfs://acer:9000/data", minPartitions);
        }
        //设置停用词
        Collections.addAll(stopWords,"a","an","the","in","on","at","for","with","to","from");
        long startTime = System.currentTimeMillis();

        JavaRDD<Tuple2<String, String>> segment2Index = null;
        //平均分段
        segment2Index = lines.flatMap(
                new FlatMapFunction<String, Tuple2<String, String>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<Tuple2<String, String>> call(String line) throws Exception {
                        String[] tokens = line.split(" ");//分词
                        for (int i = 0; i < tokens.length; ++i) {
                            tokens[i] = Tool.getCleanStr(tokens[i]);
                        }
                        List<String> segmentList = getSegment(tokens);//片段
                        List<Tuple2<String, String>> seg2Index = new ArrayList<>();
                        assert segmentList != null;
                        for (String segment : segmentList) {
                            seg2Index.add(new Tuple2<>(segment, line));
                        }
                        return seg2Index.iterator();
                    }
                });

        //3.相同segment的倒排列表合并
        JavaPairRDD<String, Iterable<String>> segment2InvertedList = segment2Index.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> t) {
                        return new Tuple2<>(t._1, t._2); //(段，倒排列表)
                    }
                }).groupByKey().partitionBy(new HashPartitioner(minPartitions));//相同key的value合并
        // 并使用哈希分区，这样哈希值为i的索引保存到索引文件part-i中，相似选择时只需查该文件即可


        //4.将倒排索引保存
        System.out.println("--------save index--------");
        String outputPath;
        if(Tool.local){
            outputPath="file:///home/mika/Desktop/mika_java/mika-classes/segment_index";
        }
        else {

            outputPath="hdfs://acer:9000/segment_index";
        }
        segment2InvertedList.saveAsTextFile(outputPath);

        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("--------总时间：%d 毫秒--------\n", usedTime);
    }
}

