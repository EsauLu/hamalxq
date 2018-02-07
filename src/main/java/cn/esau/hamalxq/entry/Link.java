package cn.esau.hamalxq.entry;

import java.util.Objects;

public class Link {

    private int pid;
    private long uid;

    public Link() {
        super();
        // TODO Auto-generated constructor stub
    }

    public Link(int pid, long uid) {
        super();
        this.pid = pid;
        this.uid = uid;
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
    
    public String toText() {
        return pid+","+uid;
    }
    
    public static Link parseLink(String text) {
        if(text==null||text.isEmpty()) {
            return null;
        }
        
        try {
            String[] params=text.split(",");
            int pid=Integer.parseInt(params[0]);
            int uid=Integer.parseInt(params[1]);
            return new Link(pid, uid);
        } catch (Exception e) {
            // TODO: handle exception
        }
        
        return null;
        
    }

    @Override
    public String toString() {
        return " (" + pid + ", " + uid + ") ";
    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        if (obj != null && obj instanceof Link) {
            Link link = (Link) obj;
            if (pid==link.getPid()&&uid==link.getUid()) {
                return true;
            }
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return Objects.hash(pid, uid);
    }

}
