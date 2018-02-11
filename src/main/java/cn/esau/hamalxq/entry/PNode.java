package cn.esau.hamalxq.entry;

import java.util.Objects;

public class PNode{

    private Node node;

    private Link link;

    public PNode() {
        super();
        // TODO Auto-generated constructor stub
    }
    
    

//    public PNode(Node node, Link link) {
//		super();
//		this.node = node;
//		this.link = link;
//	}



	public Node getNode() {
        return node;
    }

    public void setNode(Node node) {
        this.node = node;
    }

    public Link getLink() {
        return link;
    }

    public void setLink(Link link) {
        this.link = link;
    }

    @Override
    public String toString() {
        return " (" + node + "," + link + ") ";
    }

    public PNode(Node node, Link link) {
        super();
        this.node = node;
        this.link = link;
    }

    public String toText() {
        return node.toText()+" "+link.toText();
    }
    
    public static PNode parsePNode(String text) {
        
        if(text==null||text.isEmpty()) {
            return null;
        }
        
        try {
            int k=text.lastIndexOf(" ");
            Node node=Node.parseNode(text.substring(0, k).trim());
            Link link=Link.parseLink(text.substring(k).trim());
            return new PNode(node, link);
        } catch (Exception e) {
            // TODO: handle exception
        }
        
        return null;
        
    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        if (obj != null && obj instanceof PNode) {
            PNode pNode = (PNode) obj;
            if (node.equals(pNode.getNode()) && link.equals(pNode.getLink())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return Objects.hash(node, link);
    }

}
