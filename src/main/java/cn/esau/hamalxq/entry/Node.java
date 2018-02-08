package cn.esau.hamalxq.entry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.io.WritableComparable;

public class Node implements WritableComparable<Node>, Cloneable{

    private long uid;
    
    private int pid;

    private String tagName;

    private NodeType type;

    private boolean isChecked;

    private Node parent;

    private Node presib;

    private Node folsib;

    private int start;

    private int end;

    private int depth;

    private List<Node> childList;

    public Node() {
        super();
        // TODO Auto-generated constructor stub
        init();
    }

    public Node(long uid) {
        super();
        // TODO Auto-generated constructor stub
        init();
        this.uid=uid;
    }

    public Node(long uid, String tagName, NodeType type) {
        super();
        init();
        this.uid = uid;
        this.tagName = tagName;
        this.type = type;
    }

    public Node(long uid, String tagName, NodeType type, int start, int end) {
        super();
        init();
        this.uid = uid;
        this.tagName = tagName;
        this.type = type;
        this.start = start;
        this.end = end;
    }

    private void init(){

        this.type = NodeType.CLOSED_NODE;
        this.childList = new ArrayList<Node>();
    }
    
    public int getStart() {
        return start;
    }

    public void setStart(int start) {
        this.start = start;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    public boolean isChecked() {
        return isChecked;
    }

    public void setChecked(boolean isChecked) {
        this.isChecked = isChecked;
    }

    public int getPid() {
        return pid;
    }

    public void setPid(int pid) {
        this.pid = pid;
    }

    public long getUid() {
        return uid;
    }

    public void setUid(long uid) {
        this.uid = uid;
    }

    public NodeType getType() {
        return type;
    }

    public void setType(NodeType type) {
        this.type = type;
    }

    public Node getParent() {
        return parent;
    }

    public void setParent(Node parent) {
        this.parent = parent;
    }

    public Node getPresib() {
        return presib;
    }

    public void setPresib(Node presib) {
        this.presib = presib;
    }

    public Node getFolsib() {
        return folsib;
    }

    public void setFolsib(Node flosib) {
        this.folsib = flosib;
    }

//    public List<Node> getChildList() {
//        return childList;
//    }
//
//    public void setChildList(List<Node> childList) {
//        this.childList = childList;
//    }

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getChildNum() {
        return childList.size();
    }

    public void addLastChild(Node child) {
        childList.add(child);
    }

    public void addFirstChild(Node child) {
        childList.add(0, child);
    }

    public void addChildByIndex(int index, Node child) {
        childList.add(index, child);
    }
    
    public void addChilds(List<Node> childs) {
        childList.addAll(childs);
    }
    
    public List<Node> getAllChilds(){
        return this.childList;
    }

    public Node getChildByIndex(int index) {
        if (index < 0 || index >= childList.size()) {
            return null;
        }
        return childList.get(index);
    }

    public Node getFirstChild() {
        if (childList.size() == 0) {
            return null;
        }
        return childList.get(0);
    }

    public Node getLastChild() {
        if (childList.size() == 0) {
            return null;
        }
        return childList.get(childList.size() - 1);
    }
    
    public void clearChilds() {
        childList.clear();
    }

    public boolean isLeftOpenNode() {
        return NodeType.LEFT_OPEN_NODE.equals(type);
    }

    public boolean isRightOpenNode() {
        return NodeType.RIGHT_OPEN_NODE.equals(type);
    }

    public boolean isPreOpenNode() {
        return NodeType.PRE_NODE.equals(type);
    }

    public boolean isClosedNode() {
        return NodeType.CLOSED_NODE.equals(type);
    }

    public String toText() {
//        return uid + " - " + type + " " + start + " " + end;
        return uid + " " + tagName + " " + type + " " + start + " " + end;
    }

    public static Node parseNode(String text) {

        String[] fileds = text.trim().split(" ");

        if (fileds.length == 5) {

            try {

                int uid = Integer.parseInt(fileds[0]);
                String tagName = fileds[1];
                NodeType type = NodeType.parseNodeType(fileds[2]);
                int start = Integer.parseInt(fileds[3]);
                int end = Integer.parseInt(fileds[4]);
                Node node=new Node(uid, tagName, type, start, end);
                return node;

            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

        }

        return null;

    }

    @Override
    public String toString() {
        if (NodeType.CLOSED_NODE.equals(type)) {
            return " " + tagName + uid + " ";
        } else if (NodeType.LEFT_OPEN_NODE.equals(type)) {
            return " +" + tagName + uid + " ";
        }
        if (NodeType.RIGHT_OPEN_NODE.equals(type)) {
            return " " + tagName + uid + "+ ";
        } else {
            return " +" + tagName + uid + "+ ";
        }
    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub

        if (obj instanceof Node) {

            Node node = (Node) obj;

            if (node.getUid() == uid && node.tagName.equals(tagName) && node.getType().equals(type)) {
                return true;
            }

        }

        return false;
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return Objects.hash(uid, tagName, type);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        out.writeInt(pid);
        out.writeLong(uid);
        out.writeUTF(tagName);
        out.writeInt(type.getValue());
        out.writeInt(start);
        out.writeInt(end);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.pid=in.readInt();
        this.uid=in.readLong();
        this.tagName=in.readUTF();
        this.type=NodeType.parseNodeType(in.readInt());
        this.start=in.readInt();
        this.end=in.readInt();
    }

    @Override
    public int compareTo(Node o) {
        // TODO Auto-generated method stub
        long res=this.uid-o.uid;
        if(res==0) {
            return 0;
        }else if(res<0){
            return -1;
        }
        return 1;
    }
    
    
    
    @Override
    public Node clone() throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        Node newNode = (Node)super.clone();
        newNode.clearChilds();
        return newNode;
    }
    
    
    
    

//    public String toBfsString() {
//        String s = toString().trim();
//
//        String chs = "";
//        if (childList.size() > 0) {
//            for (int i = 0; i < childList.size() - 1; i++) {
//                chs += childList.get(i).toBfsString();
//                chs += ", ";
//            }
//            chs += childList.get(childList.size() - 1).toBfsString();
//
//            s += ":{" + chs + "}";
//        }
//
//        return s;
//    }

    
    
    
    
    
    
    
    
    
    
    
}
