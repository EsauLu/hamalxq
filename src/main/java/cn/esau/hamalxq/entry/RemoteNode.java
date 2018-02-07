package cn.esau.hamalxq.entry;

public class RemoteNode {
    public Node node;
    public int st;
    public int ed;
    public Link link;

    public RemoteNode(Node node, int st, int ed) {
        super();
        this.node = node;
        this.st = st;
        this.ed = ed;
    }
    
    

    public RemoteNode(Node node, int st, int ed, Link link) {
        super();
        this.node = node;
        this.st = st;
        this.ed = ed;
        this.link = link;
    }



    public Node getNode() {
        return node;
    }



    public void setNode(Node node) {
        this.node = node;
    }



    public int getSt() {
        return st;
    }



    public void setSt(int st) {
        this.st = st;
    }



    public int getEd() {
        return ed;
    }



    public void setEd(int ed) {
        this.ed = ed;
    }



    public Link getLink() {
        return link;
    }



    public void setLink(Link link) {
        this.link = link;
    }



    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return " [" + node + ", " + st + ", " + ed + "] ";
    }
}