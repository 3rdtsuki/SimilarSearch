import org.apache.spark.HashPartitioner;
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
 * @version 相似选择：给定查询q，返回倒排列表
 */

//过滤算法
enum Filter{
    Prefix,Segment
}
public class SimilarChoose {
    static double tau=0.6;
    static Filter filter=Filter.Segment;//选择过滤算法
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
        String indexPath;
        switch (filter){
            case Prefix:
                indexPath="./prefix_index/part-00000";
                break;
            case Segment:
                indexPath="./segment_index/part-00000";
                break;
            default:
                return;
        }
        JavaRDD<String> indexLines=sc.textFile(indexPath);//读取索引文件，格式为(标签,[记录1,记录2])
        String query = "Discriminative xxx for Specific Degradations in Blind Super-Resolution";//待查询字符串


        //1.切分索引表项，得到（标签，倒排列表）元组对
        HashPartitioner hp=new HashPartitioner(13);
        JavaPairRDD<String,List<String>> sig2List=indexLines.mapToPair(
                new PairFunction<String, String, List<String>>(){
                    //对于每一行
                    @Override
                    public Tuple2<String, List<String>> call(String line) throws Exception {
                        return Tool.getTuple(line);
                    }
                }
        ).partitionBy(hp);//哈希分区

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

                //优化：只查询需要的分区
                //得到片段哈希值作为待查询分区号
                List<Integer>querySegmentsHashcodes=new ArrayList<>();
                assert querySegments != null;
                System.out.println("片段对应哈希值：");
                for(String seg:querySegments){
                    querySegmentsHashcodes.add(hp.getPartition(seg));
                    System.out.println(seg+":"+hp.getPartition(seg));
                }
                //mapPartitionsWithIndex筛选需要的分区
                JavaRDD<Tuple2<String, List<String>>>sig2ListPartition=sig2List.mapPartitionsWithIndex(
                        new Function2<Integer, Iterator<Tuple2<String, List<String>>>, Iterator<Tuple2<String, List<String>>>>() {
                            @Override
                            //对于每个分区号index，执行call来生成一个迭代器，里面是一个个（片段，倒排列表）
                            public Iterator<Tuple2<String, List<String>>> call(Integer index, Iterator<Tuple2<String, List<String>>> iterator) throws Exception {
//                                //输出每个分区内容。注意运行时必须注释掉，否则迭代器迭代完会失效
//                                while (iterator.hasNext()) {
//                                    System.out.println(index + ":" + iterator.next());
//                                }
                                if (querySegmentsHashcodes.contains(index)) {//如果该分区是需要的
                                    return iterator;
                                } else {
                                    return Collections.emptyIterator();//返回一个空的iter，不能是null
                                }
                            }
                        }
                        ,false);

                JavaPairRDD<String,List<String>>sig2ListPartition2=sig2ListPartition.mapToPair(
                        (PairFunction<Tuple2<String, List<String>>, String, List<String>>) tuple2 -> tuple2
                );

                //在需要的分区内部筛选查询片段
                resultTuples=sig2ListPartition2.filter(
                        new Function<Tuple2<String, List<String>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<String, List<String>> tuple) throws Exception {
                                return querySegments.contains(tuple._1);
                            }
                        }
                );
                break;
            default:
                resultTuples=null;
        }

        //3.将候选的倒排列表中所有记录编号加入集合
        JavaRDD<String>filteredRecords=resultTuples.flatMap(
                new FlatMapFunction<Tuple2<String, List<String>>, String>() {
                    @Override
                    public Iterator<String> call(Tuple2<String, List<String>> tuple) throws Exception {
                        return tuple._2.iterator();
                    }
                }
        );
        //4.去重
        JavaPairRDD<String, Integer>uniqueRecords=filteredRecords.mapToPair(
                new PairFunction<String, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(String s) throws Exception {
                        return new Tuple2<>(s,1);
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

        //5.相似度验证
        JavaPairRDD<String,Integer>resultRecords=uniqueRecords.filter(
                new Function<Tuple2<String, Integer>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Integer> tuple) throws Exception {
                        return Tool.isSimilar(cleanQuery, tuple._1,tau);
                    }
                }
        );
        //6.输出结果
        List<Tuple2<String,Integer>>results=resultRecords.collect();
        System.out.println("Results:");
        for(Tuple2<String,Integer> res :results){
            System.out.println(res._1);
        }

        sc.close();
    }
}