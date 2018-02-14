package cn.esau.hamalxq.bsp;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Message;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.entry.Step;
import cn.esau.hamalxq.parser.PartialTreeBuilder;
import cn.esau.hamalxq.parser.XPathParser;
import cn.esau.hamalxq.query.Communication;
import cn.esau.hamalxq.query.Querier;
import cn.esau.hamalxq.utils.Utils;

public class LxqBSP extends BSP<LongWritable, Text, Text, Text, Message> {

    private Querier querier;
    
    private Map<String, Step> xpathMap;
    
    private Communication com;

    @Override
    public void setup(BSPPeer<LongWritable, Text, Text, Text, Message> peer) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(peer);

        try {
			com=new Communication(peer);
			
			querier=new Querier();
			querier.setPeer(peer);        

			HamaConfiguration conf = peer.getConfiguration();
			String xpath = conf.get("xpath");
			Path path = new Path(xpath);
			
			FileSystem fs = path.getFileSystem(conf);
			FSDataInputStream fin = fs.open(path);
			
			System.out.println(path.toString());
			
			StringBuilder sb=new StringBuilder();
			byte[] buff=new byte[1024];
			int len=0;
			while((len=fin.read(buff))!=-1) {
			    String tem=new String(buff, 0, len);
			    sb.append(tem);
			}
			xpathMap=XPathParser.getXPaths(sb.toString().split("\n"));
			
			querier.setXpathMap(xpathMap);
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}
        
    }

    @Override
    public void cleanup(BSPPeer<LongWritable, Text, Text, Text, Message> peer) throws IOException {
        // TODO Auto-generated method stub
        super.cleanup(peer);
    }

    @Override
    public void bsp(BSPPeer<LongWritable, Text, Text, Text, Message> peer) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub

        try {
			buildPartialTree(peer);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw e;
		}

    }

    private void buildPartialTree(BSPPeer<LongWritable, Text, Text, Text, Message> peer)
            throws IOException, SyncException, InterruptedException {
        
        long t1=System.currentTimeMillis();
        PartialTree pt=new PartialTreeBuilder(com, peer).buildPartialTree();
        long t2=System.currentTimeMillis();
        
        peer.write(new Text(), new Text());
        peer.write(new Text("Partial-Trees builded time out : "), new Text((t2-t1)+"ms"));
        peer.write(new Text(), new Text());
        
//        peer.write(new Text("Partial-tree : "), new Text(Utils.getTreeString(pt.getPid(), pt.getRoot())));
//        peer.write(new Text(), new Text());
        
        querier.setPt(pt);
        querier.start();
    }

}
