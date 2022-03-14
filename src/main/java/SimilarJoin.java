import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;

public class SimilarJoin {
    static double tau=0.6;
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Mika")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        JavaRDD<String> indexLines=sc.textFile("./index/part-00000");//读取索引文件，格式为(标签,[记录1,记录2])

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

        //3.记录对一一验证
        JavaRDD<Tuple2<String,String>>resultPairs=pairs.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        return Tool.isSimilar(tuple._1,tuple._2,tau);
                    }
                }
        );

        //4.输出结果
        List<Tuple2<String,String>>results=resultPairs.collect();
        System.out.println("Results:");
        for(Tuple2<String,String> res :results){
            System.out.println(res._1+","+res._2);
        }

        sc.close();
    }
}
