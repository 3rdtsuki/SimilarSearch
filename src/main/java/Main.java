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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author Mika
 * @version 相似连接
 */
public class Main {
    public static void main(String[] args) {
        int n=10;
        for(int i=0;i<4;++i){
            double tau=i*0.1+0.6;
            System.out.printf("%d,%d\n",n/((int) Math.ceil((1 - tau) / tau * n) + 1),(int) Math.ceil(tau * n) + 1);
        }

    }
}