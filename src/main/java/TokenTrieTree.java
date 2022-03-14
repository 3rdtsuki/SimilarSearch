
import org.apache.spark.sql.sources.In;

import java.util.*;

class TokenTrieNode{
    int no;//编号
    static int TokenTrieNodeNum;//节点总数
    String value;//节点的单词
    boolean isLeaf=false;
    List<TokenTrieNode>children=new ArrayList<>();//子节点
    List<Integer>childrenNumList=new ArrayList<>();//子节点的编号集合
    List<String>record=new ArrayList<>();
    TokenTrieNode(String value){
        TokenTrieNodeNum++;
        this.no=TokenTrieNodeNum;
        this.value=value;
    }
}

class TokenTrieTree{
    // 建树
    void insert(TokenTrieNode root,String record){
        String[]s=record.split(" ");
        TokenTrieNode curr=root;
        TokenTrieNode newChild;
        for (String item : s) {
            boolean isContained = false;
            for (int j = 0; j < curr.children.size(); ++j) {
                if (Objects.equals(curr.children.get(j).value, item)) {//已经有该分支
                    isContained = true;
                    curr = curr.children.get(j);
                    break;
                }
            }
            if (!isContained) {//创建新的分支，作为子节点
                newChild = new TokenTrieNode(item);
                curr.children.add(newChild);
                curr.childrenNumList.add(newChild.no);
//                System.out.printf("%s插入子节点%s\n",curr.value,item);
                curr = newChild;
            }
        }
        curr.isLeaf=true;
        //记录加入叶节点倒排列表
        curr.record.add(record);
    }
    //构建树索引文件，{节点编号，子节点编号列表，索引表项编号（如果是叶节点）}
    void buildTrieIndex(TokenTrieNode curr){
        System.out.print(curr.no+" "+curr.value+" 子节点："+curr.childrenNumList+" 倒排列表："+curr.record+"\n");

    }
    //遍历整棵树，并构建树索引和倒排索引
    void dfs(TokenTrieNode curr){
        buildTrieIndex(curr);
        if(curr.isLeaf) {//叶节点
            return;
        }
        for(int i=0;i<curr.children.size();++i){
            dfs(curr.children.get(i));
        }
    }
    //查询时遍历当前节点的子树，返回合并后的倒排列表
    List<String> dfsQuery(TokenTrieNode curr){
        List<String>recordList=new ArrayList<>();
        LinkedList<TokenTrieNode> stack=new LinkedList<>();
        stack.push(curr);
        TokenTrieNode p;
        while(stack.size()!=0){
            p=stack.poll();
            if(p.isLeaf){
                recordList.addAll(p.record);
            }
            for(TokenTrieNode child:p.children){
                stack.push(child);
            }
        }
        return recordList;
    }
    //找到query对应的叶节点curr
    TokenTrieNode searchTokenTrieNode(TokenTrieNode root,String query){
        String[]s=query.split(" ");
        TokenTrieNode curr=root;
        for (String item : s) {
            boolean isContained = false;
            for (int j = 0; j < curr.children.size(); ++j) {
                if (Objects.equals(curr.children.get(j).value, item)) {//已经有该分支
                    isContained = true;
                    curr = curr.children.get(j);
                    break;
                }
            }
            if (!isContained) {//没有该分支
                System.out.println("没有该分支！");
            }
        }
        System.out.print("找到"+query+"对应的倒排列表：");
        System.out.println(dfsQuery(curr));
        return curr;
    }

    public static void main(String[] args) {
       TokenTrieTree tree=new TokenTrieTree();
       TokenTrieNode root=new TokenTrieNode("root");
       String[] text={"we are one","we are roselia","we like lisa"};
       for(String t:text) {
           tree.insert(root,t);
       }
       tree.dfs(root);
       TokenTrieNode curr=tree.searchTokenTrieNode(root,"we");
    }

}
