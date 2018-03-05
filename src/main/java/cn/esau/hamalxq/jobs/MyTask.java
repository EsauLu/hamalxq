package cn.esau.hamalxq.jobs;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJob;
import org.apache.hama.bsp.FileInputFormat;
import org.apache.hama.bsp.FileOutputFormat;
import org.apache.hama.bsp.TextOutputFormat;

import cn.esau.hamalxq.bsp.LxqBSP;
import cn.esau.hamalxq.bsp.input.format.TagInputFormat;

public class MyTask {

    public static boolean runJob(String input, String output, String xpath, int taskNum) throws IllegalArgumentException, Exception {
        HamaConfiguration conf = new HamaConfiguration();
        
        conf.set("xpath", xpath);

        BSPJob bsp = new BSPJob(conf, MyTask.class);
        
        
        
        // Set the job name
        bsp.setJobName("Partial trees building task");
        bsp.setBspClass(LxqBSP.class);

        bsp.setInputFormat(TagInputFormat.class);
        bsp.setOutputFormat(TextOutputFormat.class);

        bsp.setOutputKeyClass(Text.class);
        bsp.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(bsp, new Path(input));
        FileOutputFormat.setOutputPath(bsp, new Path(output));

        bsp.setNumBspTask(taskNum);
        
        return bsp.waitForCompletion(true);
        
    }

}
