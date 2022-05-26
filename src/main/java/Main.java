
public class Main {
    public static void main(String[] args) {
        for (int i = 0; i < 5; ++i) {
            double tau = i * 0.1 + 0.5;
            for (int n = 5; n < 115; n+=5) {
                //片段平均长度
                int segmentNum = ((int) Math.ceil((1 - tau) / tau * n) + 1);
                System.out.printf("%f,",(float) n/ segmentNum );
            }
            System.out.println();
            for (int n = 5; n < 115; n+=5) {
                System.out.printf("%d,", n - (int) Math.ceil(tau * n) + 1);
            }
            System.out.println("\n");
        }

    }
}