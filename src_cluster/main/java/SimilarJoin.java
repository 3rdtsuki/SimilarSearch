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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Mika
 * @version 相似连接
 */
public class SimilarJoin {
    static double tau=0.8;
    static Filter filter=Filter.Prefix;//修改这里，选择过滤算法

    public static void main(String[] args) {
        tau=Double.parseDouble(args[0]);//阈值
        int minPartitions=Integer.parseInt(args[1]);//分区数

        SparkConf conf = new SparkConf()
                .setAppName("Mika");
//                .setMaster("local"); # 集群环境则注释掉该行
        JavaSparkContext sc = new JavaSparkContext(conf);
        String indexPath;
        switch (filter){
            case Prefix:
//                indexPath="file:///home/mika/Desktop/mika_java/mika-classes/prefix_index";
                indexPath="hdfs://acer:9000/prefix_index";
                break;
            case Segment:
//                indexPath="file:///home/mika/Desktop/mika_java/mika-classes/segment_index/";
                indexPath="hdfs://acer:9000/segment_index";
                break;
            default:
                return;
        }
        JavaRDD<String> indexLines=sc.textFile(indexPath,minPartitions);//读取索引文件，格式为(标签,[记录1,记录2])
        long startTime = System.currentTimeMillis();//读完索引文件后，开始计时。事实上shell中每次查询都从这开始

        //1.切分索引表项，得到（标签，倒排列表）元组对
        JavaPairRDD<String, List<String>> sig2List=indexLines.mapToPair(
                new PairFunction<String, String, List<String>>(){

                    @Override
                    public Tuple2<String, List<String>> call(String line) throws Exception {
                        return Tool.getTuple(line);
                    }
                }
        );

        //2.对每个倒排列表，将记录对加入集合
        JavaRDD<Tuple2<String,String>>pairs=sig2List.flatMap(
                new FlatMapFunction<Tuple2<String, List<String>>, Tuple2<String, String>>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<String, List<String>> tuple) throws Exception {
                        List<Tuple2<String,String>>pairList=new ArrayList<>();
                        int n=tuple._2.size();
                        for(int i=0;i<n;++i){
                            for(int j=i+1;j<n;++j){
                                pairList.add(new Tuple2<>(tuple._2.get(i),tuple._2.get(j)));
                            }
                        }
                        return pairList.iterator();
                    }
                }
        );
        //去重
        JavaPairRDD<Tuple2<String,String>,Integer>pairSet=pairs.mapToPair(
                new PairFunction<Tuple2<String, String>, Tuple2<String, String>, Integer>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, String> tuple) throws Exception {
                        return new Tuple2<>(tuple,1);
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


        //3.记录对一一验证
        JavaPairRDD<Tuple2<String,String>,Integer>resultPairs=pairSet.filter(
                new Function<Tuple2<Tuple2<String, String>, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Tuple2<String, String>, Integer> tuple) throws Exception {
                        double similarity= Tool.jaccardSimilarity(tuple._1._1,tuple._1._2);
                        return similarity>=tau;
                    }
                }
        );

        //4.输出结果
        List<Tuple2<Tuple2<String, String>, Integer>> results=resultPairs.collect();
        System.out.println("---------Results:----------");
        int cnt=0;
        for(Tuple2<Tuple2<String, String>, Integer> tuple :results){
            cnt+=1;
//            System.out.printf("%d: [(%s)(%s)]\n",cnt,tuple._1._1,tuple._1._2);
        }

        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("--------相似对数：%d --------\n", cnt);
        System.out.printf("--------总时间：%d 毫秒--------\n", usedTime);
    }
}
