package cn.esau.hamalxq.main;

import cn.esau.hamalxq.jobs.MyTask;

public class Main {
	
	//hama jar /home/esau/Desktop/workspace/hamalxq/target/hamalxq-0.0.1-SNAPSHOT.jar
    
    private static int taskNum=5;

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        
        try {

        	String xml="ex";
        	String xpath="1";
        	if(args.length==2) {
        		xml="x"+args[0];
        		xpath="x"+args[1];
        	}
            runBuildPartialTreesJob(xml, xpath);
            
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
        }

    }
    
    public static void runBuildPartialTreesJob(String xml, String xp) throws IllegalArgumentException, Exception{
        
//        String input="xml/ex";        
//        String output="output/extrees";
//        String xpath="xpath/XPaths1.txt";
        
//        String input="xml/x1";        
//        String output="output/x1";
//        String xpath="xpath/XPaths2.txt";
        
//      String input="xml/x6";        
//      String output="output/x6";
//      String xpath="xpath/XPaths2.txt";
        
		String input = "xml/"+xml;
		String output = "output/"+xml;
		String xpath = "xpath/XPaths"+xp+".txt";

		System.out.println(input);
		System.out.println(output);
		System.out.println(xpath);
        
        if(MyTask.runJob(input, output, xpath, taskNum)) {
            System.out.println("True");
        }else {
            System.out.println("False");
        }
        
    }

}
