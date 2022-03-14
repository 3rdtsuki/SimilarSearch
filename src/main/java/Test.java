import java.util.Arrays;
import java.util.BitSet;

public class Test {
    public static void main(String[] args) {
        BitSet bitSet=new BitSet();
        bitSet.set(9,true);
        bitSet.set(0,false);
        System.out.println(bitSet.get(0));
        System.out.println(bitSet.get(9));
    }
}
