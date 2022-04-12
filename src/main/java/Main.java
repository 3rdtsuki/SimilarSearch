public class Main {
    public static void main(String[] args) {
        double tau=0.6;
        int n=1;
        System.out.println((int) Math.ceil((1 - tau) / tau * n) + 1);
    }
}