package cn.esau.hamalxq.entry;

public enum NodeType {
    
    CLOSED_NODE(0), LEFT_OPEN_NODE(1), RIGHT_OPEN_NODE(2), PRE_NODE(3);
    
    private int type;
    
    private NodeType(int type) {
        // TODO Auto-generated constructor stub
        this.type=type;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
    
    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return String.valueOf(type);
    }
    
    public static NodeType parseNodeType(String s) {
        // TODO Auto-generated method stub
        
        int t=-1;
        try {
            t=Integer.parseInt(s);
        } catch (NumberFormatException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        NodeType nodeType=CLOSED_NODE;
        
        switch (t) {
        case 1:
            
            nodeType=NodeType.LEFT_OPEN_NODE;
            
            break;

        case 2:
            
            nodeType=NodeType.RIGHT_OPEN_NODE;
            
            break;

        case 3:
            
            nodeType=NodeType.PRE_NODE;
            
            break;

        default:
            
            nodeType=NodeType.CLOSED_NODE;
            
            break;
        }
        
        return nodeType;

    }
    
}































