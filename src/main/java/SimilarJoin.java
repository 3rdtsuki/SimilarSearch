import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

public class SimilarJoin {
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
        JavaRDD<Tuple2<String,String>>index2IndexRDD=sig2List.flatMap(
                new FlatMapFunction<Tuple2<String, List<String>>, Tuple2<String, String>>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<String, List<String>> tuple) throws Exception {
                        Set<Tuple2<String,String>>pairSet=new HashSet<>();
                        int n=tuple._2.size();
                        for(int i=0;i<n;++i){
                            for(int j=i+1;j<n;++j){
                                pairSet.add(new Tuple2<>(tuple._2.get(i),tuple._2.get(j)));
                            }
                        }
                        return pairSet.iterator();
                    }
                }
        );

        //相同key对应的value合并
        JavaPairRDD<String, Iterable<String>> index2IndexListRDD =index2IndexRDD.mapToPair(
                new PairFunction<Tuple2<String,String>,String,String>(){
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Tuple2<String,String> call(Tuple2<String,String> t){
                        return new Tuple2<>(t._1, t._2); //(段，倒排列表)
                    }
                }).groupByKey().sortByKey();


        //3.读取数据集
        System.out.println("开始读记录集合");
        JavaRDD<String> lines=sc.textFile("data.txt");//每条记录为index,"record"
//        JavaPairRDD<String,String>index2Record=lines.mapToPair(
//                new PairFunction<String, String, String>() {
//                    @Override
//                    public Tuple2<String, String> call(String line) throws Exception {
//                        String[]item=line.split(",");
//                        String index=item[0];
//                        String record=item[1];
//                        return new Tuple2<>(index,record);
//                    }
//                }
//        );
        //建立DataFrame
        JavaRDD<Row> index2Record = lines.map(
                new Function<String,Row>() {
                    private static final long serialVersionUID = 1L;
                    @Override
                    public Row call(String s) throws Exception {
                        return RowFactory.create(
                                s.split(",")[0],
                                s.split(",")[1]
                        );
                    }
                });

        List<StructField> asList = Arrays.asList(//确定表的属性
                DataTypes.createStructField("index", DataTypes.StringType, true),
                DataTypes.createStructField("record", DataTypes.StringType, true)
        );
        StructType schema = DataTypes.createStructType(asList);//模式
        //将RDD转为DataFrame，供SparkSQL查询
        Dataset<Row> df=sqlContext.createDataFrame(index2Record,schema);
        df.registerTempTable("table");//创建临时表
        Dataset<Row> res=sqlContext.sql(String.format("SELECT record FROM table WHERE index = '%s'", "1"));
        //遍历DataSet
        res.foreach(
                new ForeachFunction<Row>() {
                    @Override
                    public void call(Row row) throws Exception {
                        String record=row.getString(0);
                        System.out.println(record);
                    }
                }
        );

        //4.对集合中的每一对记录编号进行验证
        //4.1使用key:[id1,id2,idn]验证法
        List<Tuple2<String, Iterable<String>>> index2IndexList=index2IndexListRDD.collect();
        for(Tuple2<String, Iterable<String>>tuple:index2IndexList){
            String index1= tuple._1;
            Iterable<String>indexList= tuple._2;
            Dataset<Row> r1=sqlContext.sql(String.format("SELECT record FROM table WHERE index = '%s'", index1));
            for (String index2:indexList){
                Dataset<Row> r2=sqlContext.sql(String.format("SELECT record FROM table WHERE index = '%s'", index2));
            }
        }
        //4.2使用(key:id)验证法
    }
}
