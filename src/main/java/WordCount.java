import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WordCount {
    //数据预处理
    static String dataPreprocess(String str){
        str=str.toLowerCase();//大写转小写
        str=str.replaceAll("[?:!.,;]*", "");//删除标点符号
        return str;
    }
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String>lines=sc.textFile("data.txt");
        //RDD1：数据预处理，并分词。只要是输出数量大于输入就是用flatMap得到JavaRDD
        JavaRDD<String>words=lines.flatMap(
                new FlatMapFunction<String,String>(){
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Iterator<String> call(String line) throws Exception {
                        line=dataPreprocess(line);
                        String[] tokens=line.split(" ");
                        return Arrays.asList(tokens).iterator();
                    }
                });
        //RDD2：生成映射（单词，1）的元组。只要是输出数量=输入，就是得到JavaPairRDD
        JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
        //RDD3：合并相同的单词对应的元组，即频度相加
        JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        //.collect()将RDD转成List
        List<Tuple2<String, Integer>> output = counts.collect();
        for (Tuple2<String, Integer> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
    }
}