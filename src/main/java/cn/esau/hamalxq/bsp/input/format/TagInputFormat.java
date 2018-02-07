package cn.esau.hamalxq.bsp.input.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.FileInputFormat;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.RecordReader;

import cn.esau.hamalxq.bsp.input.reader.TagRecordReader;
import cn.esau.hamalxq.bsp.input.split.TagSplit;

public class TagInputFormat extends FileInputFormat<LongWritable, Text> {

    public InputSplit[] getSplits(BSPJob job, int numBspTask) throws IOException {
        // TODO Auto-generated method stub        
        
        Configuration conf = job.getConfiguration();

        FileSystem fs = FileSystem.get(conf);
        Path inputPath = getInputPath(job);
        FileStatus[] fileStatus = fs.listStatus(inputPath);

        long totalLen = 0;
        for (FileStatus file : fileStatus) {
            if (file.isFile()) {
                totalLen += file.getLen();
            }
        }

        int splitNum = job.getNumBspTask();
        long splitSize = totalLen / splitNum + 1;

        List<InputSplit> splits = new ArrayList<InputSplit>();
        for (FileStatus file : fileStatus) {
            if (file.isFile()) {
                splits.addAll(getSplitsByFile(file, splitSize));
            }
        }
        
        return splits.toArray(new InputSplit[splits.size()]);
    }

    public RecordReader<LongWritable, Text> getRecordReader(InputSplit split, BSPJob job) throws IOException {
        // TODO Auto-generated method stub
        return new TagRecordReader((TagSplit)split);
    }

    private List<InputSplit> getSplitsByFile(FileStatus file, long splitSize) {
        List<InputSplit> splits = new ArrayList<InputSplit>();
        long fileLen = file.getLen();

        long start = 0;

        while (start < fileLen) {
            TagSplit split = new TagSplit(file.getPath(), start, Math.min(splitSize, fileLen - start));
            splits.add(split);
            start += splitSize;
        }

        return splits;
    }

    public static void setInputPath(BSPJob job, Path path) throws Exception {
        FileInputFormat.setInputPaths(job, path);
    }

    public static Path getInputPath(BSPJob job) {
        Path[] paths=FileInputFormat.getInputPaths(job);
        if(paths==null) {
            return null;
        }
        return paths[0];
    }

}
