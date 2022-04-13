import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.List;

//暴力查找
public class BruteForceSelect {
    static double tau=0.6;
    public static void main(String[] args) {
        tau=Double.parseDouble(args[0]);//阈值
        int minPartitions=Integer.parseInt(args[1]);//分区数
        String query = args[2];//待查询字符串

        SparkConf conf = new SparkConf()
                .setAppName("Mika");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("hdfs://acer:9000/data",minPartitions);
        String cleanQuery = Tool.getCleanStr(query);

        long startTime = System.currentTimeMillis();
        //5.相似度验证
        JavaRDD<String> resultRecords = lines.filter(
                new Function<String, Boolean>() {
                    @Override
                    public Boolean call(String s) throws Exception {
                        return Tool.jaccardSimilarity(Tool.getCleanStr(s),cleanQuery) >=tau;
                    }
                }
        );
        List<String> result=resultRecords.collect();
        int cnt=0;
        for (String line:result){
            cnt+=1;
            System.out.printf("%d: %s\n",cnt,line);
        }
        long endTime = System.currentTimeMillis();
        long usedTime = endTime - startTime;

        sc.close();

        System.out.printf("总时间：%d 毫秒", usedTime);
    }
}
