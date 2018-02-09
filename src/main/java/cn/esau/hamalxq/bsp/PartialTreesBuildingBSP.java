package cn.esau.hamalxq.bsp;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
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

import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.NodeType;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.entry.Step;
import cn.esau.hamalxq.parser.PartialTreeBuilder;
import cn.esau.hamalxq.parser.XPathParser;
import cn.esau.hamalxq.query.Querier;

public class PartialTreesBuildingBSP extends BSP<LongWritable, Text, LongWritable, Text, Node> {

    private int pid;

    private Querier querier;
    
    private Map<String, Step> xpathMap;

    @Override
    public void setup(BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(peer);
        pid = peer.getPeerIndex();
        
        querier=new Querier();
        querier.setPeer(peer);        

        HamaConfiguration conf = peer.getConfiguration();
        String xpath = conf.get("xpath");
        Path path = new Path(xpath);
        
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream fin = fs.open(path);
        
        StringBuilder sb=new StringBuilder();
        byte[] buff=new byte[1024];
        int len=0;
        while((len=fin.read(buff))!=-1) {
            String tem=new String(buff, 0, len);
            sb.append(tem);
        }
        
        xpathMap=XPathParser.getXPaths(sb.toString().split("\n"));
        
        querier.setXpathMap(xpathMap);
        
    }

    @Override
    public void cleanup(BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer) throws IOException {
        // TODO Auto-generated method stub
        super.cleanup(peer);

        PartialTree pt = querier.getPt();
        Node root = pt.getRoot();

        if (root == null) {
            return;
        }

        Deque<Node> que = new ArrayDeque<>();
        que.addLast(root);

        long c = 0;

        while (!que.isEmpty()) {

            Node node = que.removeFirst();

            if (NodeType.CLOSED_NODE.equals(node.getType())) {
                peer.write(new LongWritable(c++), new Text(node.toString()));
            } else {
                String s = node.toString();
                s = s.substring(0, s.length() - 1);
                peer.write(new LongWritable(c++), new Text(s + "(" + node.getStart() + ", " + node.getEnd() + ") "));
            }

            for (int j = 0; j < node.getChildNum(); j++) {
                que.addLast(node.getChildByIndex(j));
            }

        }

    }

    @Override
    public void bsp(BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub

        buildPartialTree(peer);

    }

    private void buildPartialTree(BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer)
            throws IOException, SyncException, InterruptedException {
        querier.setPt(PartialTreeBuilder.buildPartialTree(pid, peer));
        try {
            querier.start();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
