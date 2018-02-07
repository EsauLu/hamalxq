package cn.esau.hamalxq.bsp.input.split;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.InputSplit;

public class TagSplit implements InputSplit {
    
    private Path path;

    private long start;

    private long lenght;
    
    public TagSplit() {
        // TODO Auto-generated constructor stub
    }

    public TagSplit(Path path, long start, long lenght) {
        super();
        this.path = path;
        this.start = start;
        this.lenght = lenght;
    }

    public void readFields(DataInput in) throws IOException {
        // TODO Auto-generated method stub
        this.path = new Path(Text.readString(in));
        this.start = in.readLong();
        this.lenght = in.readLong();
    }

    public void write(DataOutput out) throws IOException {
        // TODO Auto-generated method stub
        Text.writeString(out, path.toString());
        out.writeLong(start);
        out.writeLong(lenght);
    }

    public long getLength() throws IOException {
        // TODO Auto-generated method stub
        return lenght;
    }

    public String[] getLocations() throws IOException {
        // TODO Auto-generated method stub
        return new String[] {};
    }

    public Path getPath() {
        return path;
    }

    public void setPath(Path path) {
        this.path = path;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getLenght() {
        return lenght;
    }

    public void setLenght(long lenght) {
        this.lenght = lenght;
    }

}
