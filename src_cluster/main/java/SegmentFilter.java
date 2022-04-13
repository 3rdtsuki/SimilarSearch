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
 * @version 基于分段过滤的方法
 */
public class SegmentFilter {
    //相似度阈值
    static double tau = 0.8;

    //分段方法
    enum GetSegmentMethod {
        Ordinary, Polling
    }
    //程序使用的分段方法：默认为平均分段；轮询分段需要建立频度表，以便相似选择时对查询分段，未实现
    static GetSegmentMethod getSegmentMethod = GetSegmentMethod.Ordinary;

    static List<String> getSegment(String[] tokens, GetSegmentMethod method) {
        switch (method) {
            case Polling:
                return getSegmentByPolling(tokens);
            case Ordinary:
                return getSegmentOrdinary(tokens);
            default:
                return null;
        }
    }

    //平均分段法
    static List<String> getSegmentOrdinary(String[] tokens) {
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
            segments.add(sb.toString());
        }
        return segments;
    }

    //基于频率的轮询调度分段方法，如0,1,2,3,3,2,1,0
    static List<String> getSegmentByPolling(String[] tokens) {
        int n = tokens.length;
        int segmentNum = (int) Math.ceil((1 - tau) / tau * n) + 1;//段数
        String[] segments = new String[segmentNum];
        for (int i = 0; i < n; ++i) {
            int offset = i % segmentNum;
            if ((i / segmentNum) % 2 == 0) {
                if (segments[offset] == null) {
                    segments[offset] = tokens[i];
                } else {
                    segments[offset] += " " + tokens[i];
                }
            } else {
                segments[segmentNum - offset - 1] += " " + tokens[i];
            }
        }
        return Arrays.asList(segments);
    }

    public static void main(String[] args) throws IOException {
        tau=Double.parseDouble(args[0]);//阈值
        int minPartitions=Integer.parseInt(args[1]);//分区数

        SparkConf conf = new SparkConf()
                .setAppName("Mika");
//                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
//        JavaRDD<String> lines = sc.textFile("data.txt");
        JavaRDD<String> lines = sc.textFile("hdfs://acer:9000/data",minPartitions);

        long startTime = System.currentTimeMillis();

        JavaRDD<Tuple2<String, String>> segment2Index = null;
        //平均分段
        if (getSegmentMethod == GetSegmentMethod.Ordinary) {
            segment2Index = lines.flatMap(
                    new FlatMapFunction<String, Tuple2<String, String>>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Iterator<Tuple2<String, String>> call(String line) throws Exception {
                            String[] tokens = line.split(" ");//分词
                            for (int i = 0; i < tokens.length; ++i) {
                                tokens[i] = Tool.getCleanStr(tokens[i]);
                            }
                            List<String> segmentList = getSegment(tokens, getSegmentMethod);//片段
                            List<Tuple2<String, String>> seg2Index = new ArrayList<>();
                            assert segmentList != null;
                            for (String segment : segmentList) {
                                seg2Index.add(new Tuple2<>(segment, line));
                            }
                            return seg2Index.iterator();
                        }
                    });
        }
        //基于词频轮询分段
        else if (getSegmentMethod == GetSegmentMethod.Polling) {
            //1.分词，并统计词频
            JavaRDD<String> allTokens = lines.flatMap(new FlatMapFunction<String, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Iterator<String> call(String line) throws Exception {
                    String[] tokens = line.split(" ");//分词
                    List<String> tokenList = new ArrayList<>();
                    for (String token : tokens) {
                        tokenList.add(Tool.getCleanStr(token));
                    }
                    return tokenList.iterator();//不同line获得的结果会通过flatMap自动拼成RDD
                }
            });
            JavaPairRDD<String, Integer> ones = allTokens.mapToPair(s -> new Tuple2<>(s, 1));
            JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
            Map<String, Integer> word2Count = counts.collectAsMap();


            //2.生成分段到记录的映射(segment,record)
            segment2Index = lines.flatMap(
                    new FlatMapFunction<String, Tuple2<String, String>>() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Iterator<Tuple2<String, String>> call(String record) {
                            String[] tokens = Tool.getCleanStr(record).split(" ");//分词
                            for (int i = 0; i < tokens.length; ++i) {
                                tokens[i] = Tool.getCleanStr(tokens[i]);
                            }

                            //每条记录元素按频率排序，若频率相同则按字典序
                            Arrays.sort(tokens, new Comparator<String>() {
                                @Override
                                public int compare(String o1, String o2) {
                                    int delta = word2Count.get(o1) - word2Count.get(o2);
                                    if (delta < 0) {
                                        return -1;
                                    } else if (delta > 0) {
                                        return 1;
                                    } else {
                                        return o1.compareTo(o2);
                                    }
                                }
                            });
                            List<String> segments = getSegment(tokens, getSegmentMethod);//分段
                            List<Tuple2<String, String>> segment2Index = new ArrayList<>();
                            assert segments != null;
                            for (String s : segments) {
                                segment2Index.add(new Tuple2<>(s, record));//（分段，记录编号）
                            }
                            return segment2Index.iterator();
                        }
                    });
        }
        //3.相同segment的倒排列表合并
        JavaPairRDD<String, Iterable<String>> segment2InvertedList = segment2Index.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> t) {
                        return new Tuple2<>(t._1, t._2); //(段，倒排列表)
                    }
                }).groupByKey().sortByKey();//


        //4.将倒排索引保存
        System.out.println("--------save index--------");
//        String outputPath="file:///home/mika/Desktop/mika_java/mika-classes/segment_index";
        String outputPath="hdfs://acer:9000/segment_index";
        segment2InvertedList.saveAsTextFile(outputPath);
//        List<Tuple2<String, Iterable<String>>> output = segment2InvertedList.collect();
//        for (Tuple2<String, Iterable<String>> tuple : output) {
//            System.out.println(tuple._1() + ":" + tuple._2());
//        }

        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("--------总时间：%d 毫秒--------\n", usedTime);
    }
}

