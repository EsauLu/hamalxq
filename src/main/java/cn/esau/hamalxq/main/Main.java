package cn.esau.hamalxq.main;

import cn.esau.hamalxq.jobs.PartialTreesBuildTask;

public class Main {
    
    private static int taskNum=5;

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
        try {
            
            long t1=0;
            long t2=0;
            
            System.out.println("Building Partial Trees...");
            t1=System.currentTimeMillis();
            runBuildPartialTreesJob();
            t2=System.currentTimeMillis();
            System.out.println("Build partial-trees time out : "+(t2-t1)+" ms");
            
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }
    
    public static void runBuildPartialTreesJob() throws IllegalArgumentException, Exception{
        
        String input="xml/ex";        
        String output="output/extrees";
        String xpath="xpath/XPaths1.txt";
        
//        String input="xml/x1";        
//        String output="output/x1";
//        String xpath="xpath/XPaths2.txt";
        
//        String input="xml/x6";        
//        String output="output/x6";
//        String xpath="xpath/XPaths2.txt";
        
        if(PartialTreesBuildTask.runJob(input, output, xpath, taskNum)) {
            System.out.println("True");
        }else {
            System.out.println("False");
        }
        
    }

}
