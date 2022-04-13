import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Mika
 * @version 相似连接
 */
public class Main {
    static double tau=0.8;
    static Filter filter=Filter.Prefix;//修改这里，选择过滤算法

    public static void main(String[] args) {

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
        JavaRDD<String> indexLines=sc.textFile(indexPath,256);//读取索引文件，格式为(标签,[记录1,记录2])
        long startTime = System.currentTimeMillis();//读完索引文件后，开始计时。事实上shell中每次查询都从这开始

        System.out.println("========="+indexLines.collect().size());

        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("--------总时间：%d 毫秒--------\n", usedTime);
    }
}
