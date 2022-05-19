
public class Main {
    public static void main(String[] args) {
        double tau=0.9;
        for (int i = 5; i <=110; ++i) {
//            int prefixLen=i-(int) Math.ceil(tau*i)+1;//前缀长度
            int segmentNum = (int) Math.ceil((1 - tau) / tau * i) + 1;//段数
            float temp = (float) i / segmentNum;
            System.out.printf("%f,",temp);
        }

    }
}