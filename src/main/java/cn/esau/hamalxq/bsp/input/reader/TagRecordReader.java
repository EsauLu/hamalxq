package cn.esau.hamalxq.bsp.input.reader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.RecordReader;

import cn.esau.hamalxq.bsp.input.split.TagSplit;

public class TagRecordReader implements RecordReader<LongWritable, Text> {

    private long start;
    private long end;

    private FSDataInputStream fin;
    
    public TagRecordReader() {
        // TODO Auto-generated constructor stub
    }
    
    public TagRecordReader(TagSplit split) throws IOException {
        // TODO Auto-generated constructor stub
        start = split.getStart();

        end = start + split.getLength();

        Path file = split.getPath();

        FileSystem fs = file.getFileSystem(new Configuration());

        fin = fs.open(split.getPath());

        fin.seek(start);

    }

    public boolean next(LongWritable key, Text value) throws IOException {
        // TODO Auto-generated method stub
        if (fin.getPos() < end) {

            if(value==null) {
                throw new NullPointerException("Parameter key should not be a null value.");
            }

            if(key==null) {
                throw new NullPointerException("Parameter value should not be a null value.");
            }
            
            return readTag(key, value);

        }
        
        return false;
    }

    public LongWritable createKey() {
        // TODO Auto-generated method stub
        return new LongWritable();
    }

    public Text createValue() {
        // TODO Auto-generated method stub
        return new Text();
    }

    public long getPos() throws IOException {
        // TODO Auto-generated method stub
        return fin.getPos();
    }

    public void close() throws IOException {
        // TODO Auto-generated method stub
        fin.close();
    }

    public float getProgress() throws IOException {
        // TODO Auto-generated method stub
        return ((fin.getPos() - start) / (float) (end - start));
    }


    private boolean readTag(LongWritable key, Text value) throws IOException {

        
        try {
            
            long pos = -1;
            while (fin.getPos() < end) {
                char ch = (char)fin.readByte();
                if (ch == '<') {
                    pos = fin.getPos();
                    break;
                }
            }

            if (pos == -1) {
                return false;
            }

            key.set(pos);
            
            StringBuilder sb = new StringBuilder();
            while (true) {
                char ch = (char)fin.readByte();
                if (ch == '>') {
                    break;
                }
                sb.append(ch);
            }
            value.set(sb.toString());
            
            return true;
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return false;
    }

    
}
