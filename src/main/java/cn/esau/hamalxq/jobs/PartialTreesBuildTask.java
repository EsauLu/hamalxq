package cn.esau.hamalxq.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.FileInputFormat;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import cn.esau.hamalxq.bsp.PartialTreesBuildingBSP;
import cn.esau.hamalxq.bsp.input.format.TagInputFormat;

public class PartialTreesBuildTask {

    public static boolean runJob(String input, String output, int taskNum) throws IllegalArgumentException, Exception {
        HamaConfiguration conf = new HamaConfiguration();

        BSPJob bsp = new BSPJob(conf, PartialTreesBuildTask.class);
        // Set the job name
        bsp.setJobName("Partial trees building task");
        bsp.setBspClass(PartialTreesBuildingBSP.class);

        bsp.setInputFormat(TagInputFormat.class);
        bsp.setOutputFormat(TextOutputFormat.class);

        bsp.setOutputKeyClass(LongWritable.class);
        bsp.setOutputValueClass(Text.class);

//        FileInputFormat.setInputPaths(bsp, "output/extrees");
        FileInputFormat.setInputPaths(bsp, new Path(input));
        FileOutputFormat.setOutputPath(bsp, new Path(output));

        bsp.setNumBspTask(taskNum);
        
        return bsp.waitForCompletion(true);
        
    }

}
