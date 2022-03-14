import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Mika
 * @version 基于前缀过滤的算法
 */
public class PrefixFilter {
    //相似度阈值
    static double tau=0.6;
    //获取前缀
    static String getPrefix(String line){
        List<String>tokenList= Arrays.asList(line.split(" "));
        Collections.sort(tokenList);//字典序排序
        int len=tokenList.size();//记录长度
        int prefixLen=len-(int) Math.ceil(tau*len)+1;//前缀长度
        StringBuilder sb=new StringBuilder("");
        for(int i=0;i<prefixLen-1;++i){
            sb.append(tokenList.get(i));
            sb.append(" ");
        }
        sb.append(tokenList.get(prefixLen-1));
        return sb.toString();
    }
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Mika")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines=sc.textFile("data.txt");//每条记录为index,"record"

        //RDD1.每条记录内部元素按照字典序排序，然后选取前缀，得到（前缀，记录）对
        JavaPairRDD<String,String> prefix2Record=lines.mapToPair(
                new PairFunction<String,String, String>(){
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String,String> call(String record) throws Exception {
                String prefix=getPrefix(Tool.getCleanStr(record));//获取前缀

                return new Tuple2<>(prefix,record);
            }
        });

        //RDD2.合并所有相同前缀的倒排列表，并哈希分区
        JavaPairRDD<String, Iterable<String>> prefix2InvertedList=prefix2Record.mapToPair(
                new PairFunction<Tuple2<String,String>,String,String>(){
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String,String> call(Tuple2<String,String> t){
                        return new Tuple2<>(t._1, t._2); //(段，倒排列表)
                    }
                }).groupByKey().sortByKey();//哈希分区.partitionBy(new HashPartitioner(4))

        //将RDD保存到index目录
//        String outputPath="index";
//        prefix2InvertedList.saveAsTextFile(outputPath);

        for (Tuple2<String, Iterable<String>> tuple : prefix2InvertedList.collect()) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        sc.close();
    }
}
