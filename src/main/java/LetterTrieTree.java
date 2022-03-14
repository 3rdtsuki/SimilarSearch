//前缀树节点
class LetterTrieNode{
    String value;//每个节点存一个字符串
    LetterTrieNode[]childs;//每个节点有26个儿子
    boolean isEnd;//是否为一个标签的末尾
    LetterTrieNode(String c){
        value=c;
        childs=new LetterTrieNode[26];
        isEnd=false;
    }
    //插入串
    void insert(String s){
        LetterTrieNode curr=this;
        for(int i=0;i<s.length();++i){
            int index=s.charAt(i)-'a';
            if(curr.childs[index]==null){
                curr.childs[index]=new LetterTrieNode(s.substring(0,i+1));
            }
            curr=curr.childs[index];
        }
        curr.isEnd=true;
    }
    //查找前缀为s的串
    void search(String s){
        LetterTrieNode curr=this;
        for(int i=0;i<s.length();++i) {
            int index = s.charAt(i) - 'a';
            if (curr.childs[index] == null) {
                System.out.println("no match!");
                return;
            }
            curr=curr.childs[index];
        }
        dfs(curr);
    }
    //深搜节点
    void dfs(LetterTrieNode curr){
        if(curr==null){
            return;
        }
        if(curr.isEnd) {
            System.out.println(curr.value);
        }
        for(int i=0;i<curr.childs.length;++i){
            dfs(curr.childs[i]);
        }
    }

}
public class LetterTrieTree{
    public static void main(String[] args) {
        LetterTrieNode root=new LetterTrieNode("");
        String[] data={"nku","tju","nju","ncu"};
        for(String s:data){
            root.insert(s);
        }
        root.search("n");
    }
}