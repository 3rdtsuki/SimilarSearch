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
 * @version 相似连接
 */
public class SimilarJoin {
    static double tau=0.6;
    static int minPartitions=1;//分区数

    enum Filter{
        Prefix,Segment
    }
    static Filter filter=Filter.Prefix;//使用哪种索引

    public static void main(String[] args) {
        SparkConf conf;
        JavaSparkContext sc;
        String indexPath;
        if(Tool.local) {
            conf = new SparkConf()
                    .setAppName("Mika")
                    .setMaster("local"); // 集群环境则注释掉该行
            sc = new JavaSparkContext(conf);
            switch (filter){
                case Prefix:
                    indexPath="file:///home/mika/Desktop/mika_java/mika-classes/prefix_index";
                    break;
                case Segment:
                    indexPath="file:///home/mika/Desktop/mika_java/mika-classes/segment_index/";
                    break;
                default:
                    return;
            }
        }
        else{
            tau = Double.parseDouble(args[0]);
            minPartitions = Integer.parseInt(args[1]);
            conf = new SparkConf()
                    .setAppName("Mika");
            sc = new JavaSparkContext(conf);
            switch (filter){
                case Prefix:
                    indexPath="hdfs://acer:9000/prefix_index";
                    break;
                case Segment:
                    indexPath="hdfs://acer:9000/segment_index";
                    break;
                default:
                    return;
            }
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
        HashSet<String>set=new HashSet<>();
        JavaRDD<Tuple2<String,String>>pairs=sig2List.flatMap(
                new FlatMapFunction<Tuple2<String, List<String>>, Tuple2<String, String>>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<String, List<String>> tuple) throws Exception {
                        List<Tuple2<String,String>>pairList=new ArrayList<>();
                        int n=tuple._2.size();
                        for(int i=0;i<n;++i){
                            for(int j=i+1;j<n;++j){
                                Tuple2<String,String>tuple2=new Tuple2<>(tuple._2.get(i),tuple._2.get(j));
                                String s=tuple2.toString();
                                if(set.contains(s))continue;
                                else {
                                    pairList.add(tuple2);
                                    set.add(s);
                                }
                            }
                        }
                        return pairList.iterator();
                    }
                }
        );
        //3.记录对一一验证
        JavaRDD<Tuple2<String,String>>resultPairs=pairs.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        double similarity= Tool.jaccardSimilarity(tuple._1,tuple._1);
                        return similarity>=tau;
                    }
                }
        );
        //4.输出结果
        List<Tuple2<String, String>> results=resultPairs.collect();
        System.out.println("---------Results:----------");
        int cnt=0;
        for(Tuple2<String, String> tuple :results){
            cnt+=1;
//            System.out.println(tuple._1+","+tuple._2);
        }

        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("--------相似对数：%d --------\n", cnt);
        System.out.printf("--------总时间：%d 毫秒--------\n", usedTime);
    }
}
