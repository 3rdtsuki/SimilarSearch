import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Tool {
    //用正则表达式从索引表项中提取标签和倒排列表，格式为(标签,[记录1,记录2])
    static Tuple2<String,List<String>> getTuple(String line){
        Pattern pattern=Pattern.compile("\\((.*),\\[(.*)\\]\\)");
        Matcher matcher=pattern.matcher(line);
        String sig;
        List<String>recordList;
        if(matcher.find()) {
            sig = matcher.group(1);
            recordList = Arrays.asList(matcher.group(2).split(","));
            return new Tuple2<>(sig,recordList);
        }
        System.err.println("ERROR: 切分索引表项出错！");
        return null;
    }
    //字符串预处理，删除标点符号，大写转小写
    static String getCleanStr(String line){
        return line.replaceAll("[?:!.,;]*", "").toLowerCase();
    }
    //交集大小
    static int intersectionSize(List<String> list1,List<String> list2){
        list1.retainAll(list2);
        return list1.size();
    }
    //相似度验证
    static boolean isSimilar(String string1,String string2,double tau) {
        String s1=getCleanStr(string1);
        String s2=getCleanStr(string2);
        List<String> list1=new ArrayList<>(Arrays.asList(s1.split(" ")));
        List<String>list2=new ArrayList<>(Arrays.asList(s2.split(" ")));

        int intersectionSize=intersectionSize(list1,list2);
        int unionSize=list1.size()+ list2.size()-intersectionSize;
        double jaccard=(double) intersectionSize/unionSize;
        System.out.printf("%.2f %s\n",jaccard,s2);
        return jaccard>=tau;
    }

}
