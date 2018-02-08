package cn.esau.hamalxq.entry;

public class Tag {

    private String name;
    private TagType type;
    private int tid;

    public Tag(String name, TagType type) {
        super();
        this.name = name;
        this.type = type;
    }

    public Tag() {
        super();
        // TODO Auto-generated constructor stub
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public TagType getType() {
        return type;
    }

    public void setType(TagType type) {
        this.type = type;
    }

    public int getTid() {
        return tid;
    }

    public void setTid(int tid) {
        this.tid = tid;
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        if (TagType.START.equals(type)) {
            return " <" + name + ">";
        } else if (TagType.END.equals(type)) {
            return " </" + name + ">";
        } else {
            return "<" + name + "/>";
        }
    }

}
