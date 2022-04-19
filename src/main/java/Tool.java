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
            recordList = Arrays.asList(matcher.group(2).split(", "));//注意除去逗号后面的空格
            return new Tuple2<>(sig,recordList);
        }
        System.err.println("ERROR: 切分索引表项出错！");
        return null;
    }
    //字符串预处理，删除标点符号，大写转小写
    static String getCleanStr(String line){
        return line.replaceAll("[\\pP\\p{Punct}]", "").toLowerCase();
    }
    //交集大小
    static int intersectionSize(List<String> list1,List<String> list2){
        list1.retainAll(list2);
        return list1.size();
    }
    //相似度验证
    static double jaccardSimilarity(String string1,String string2) {
        String s1 = getCleanStr(string1);
        String s2 = getCleanStr(string2);
        List<String> list1 = new ArrayList<>(Arrays.asList(s1.split(" ")));
        List<String> list2 = new ArrayList<>(Arrays.asList(s2.split(" ")));
        int n1 = list1.size();
        int n2 = list2.size();

        int intersectionSize = intersectionSize(list1, list2);
        int unionSize = n1 + n2 - intersectionSize;

        return (double) intersectionSize/unionSize;
    }

}
