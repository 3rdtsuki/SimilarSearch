import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Mika
 * @version 相似选择：给定查询q，返回倒排列表
 */

//过滤算法
enum Filter{
    Prefix,Segment
}
public class SimilarChoose {
    static double tau=0.6;
    static Filter filter=Filter.Prefix;//选择过滤算法
    //判断前缀是否重叠
    static boolean isOverlapped(String s1,String s2) {
        List<String> list1=new ArrayList<>(Arrays.asList(s1.split(" ")));
        List<String>list2=new ArrayList<>(Arrays.asList(s2.split(" ")));
        return Tool.intersectionSize(list1,list2)>0;
    }



    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Mika")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext=new SQLContext(sc);
        JavaRDD<String> indexLines=sc.textFile("./index/part-00000");//读取索引文件，格式为(标签,[记录1,记录2])
        String query = "Discriminative xxx for Specific Degradations in Blind Super-Resolution";//待查询字符串


        //1.切分索引表项，得到（标签，倒排列表）元组对
        JavaPairRDD<String,List<String>> sig2List=indexLines.mapToPair(
                new PairFunction<String, String, List<String>>(){

                    @Override
                    public Tuple2<String, List<String>> call(String line) throws Exception {
                        return Tool.getTuple(line);
                    }
                }
        );

        //2.过滤，获得倒排列表集，有前缀和片段两种算法
        String cleanQuery = Tool.getCleanStr(query);
        JavaPairRDD<String, List<String>> resultTuples;
        switch (filter) {
            //比较前缀看是否重叠
            case Prefix:
                System.out.println("开始比较前缀");
                String queryPrefix = PrefixFilter.getPrefix(cleanQuery);//获得查询的前缀
                resultTuples = sig2List.filter(//筛选前缀重叠的
                        new Function<Tuple2<String, List<String>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<String, List<String>> tuple) throws Exception {
                                //前缀是否重叠
                                return isOverlapped(queryPrefix, tuple._1);
                            }
                        }
                );
                break;
            //比较片段看是否相同
            case Segment:
                System.out.println("开始比较片段");
                String[]tokens=Tool.getCleanStr(cleanQuery).split(" ");//分词
                List<String>querySegments=SegmentFilter.getSegment(tokens,SegmentFilter.getSegmentMethod);
                resultTuples=sig2List.filter(
                        new Function<Tuple2<String, List<String>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<String, List<String>> tuple) throws Exception {
                                assert querySegments != null;
                                return querySegments.contains(tuple._1);//如果倒排索引标签在查询标签集合中
                            }
                        }
                );
                break;
            default:
                resultTuples=null;
        }

        //将候选的倒排列表中所有记录编号加入集合
        Set<String> idSet=new HashSet<>();
        for(Tuple2<String, List<String>> resultTuple:resultTuples.collect()){
            idSet.addAll(resultTuple._2);
        }

        //3.读取所有记录
        System.out.println("开始读记录集合");
        JavaRDD<String> lines=sc.textFile("data.txt");//每条记录为index,"record"
        JavaPairRDD<String,String>index2Record=lines.mapToPair(
                new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String line) throws Exception {
                        String[]item=line.split(",");
                        String index=item[0];
                        String record=item[1];
                        return new Tuple2<>(index,record);
                    }
                }
        );
        //4.根据集合中所有编号找到对应的记录(index,record)
        JavaPairRDD<String,String>filteredRecord=index2Record.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        return idSet.contains(tuple._1);
                    }
                }
        );
        //相似度验证
        JavaPairRDD<String,String>result=filteredRecord.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        return Tool.isSimilar(cleanQuery, Tool.getCleanStr(tuple._2),tau);
                    }
                }
        );
        //5.输出结果
        List<Tuple2<String,String>>results=result.collect();
        System.out.println("Results:");
        for(Tuple2<String,String> tuple :results){
            System.out.println(tuple._2);
        }

        sc.close();
    }
}