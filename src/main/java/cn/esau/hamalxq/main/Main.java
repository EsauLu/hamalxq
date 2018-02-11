package cn.esau.hamalxq.main;

import cn.esau.hamalxq.jobs.MyTask;

public class Main {
	
	//hama jar /home/esau/Desktop/workspace/hamalxq/target/hamalxq-0.0.1-SNAPSHOT.jar
    
    private static int taskNum=5;

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
        try {
            
            runBuildPartialTreesJob();
            
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }
    
    public static void runBuildPartialTreesJob() throws IllegalArgumentException, Exception{
        
//        String input="xml/ex";        
//        String output="output/extrees";
//        String xpath="xpath/XPaths1.txt";
        
//        String input="xml/x1";        
//        String output="output/x1";
//        String xpath="xpath/XPaths2.txt";
        
        String input="xml/x6";        
        String output="output/x6";
        String xpath="xpath/XPaths2.txt";
        
        if(MyTask.runJob(input, output, xpath, taskNum)) {
            System.out.println("True");
        }else {
            System.out.println("False");
        }
        
    }

}
