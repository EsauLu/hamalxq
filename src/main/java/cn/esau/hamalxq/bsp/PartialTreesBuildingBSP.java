package cn.esau.hamalxq.bsp;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.parser.PartialTreeBuilder;
import cn.esau.hamalxq.query.Querier;

public class PartialTreesBuildingBSP extends BSP<LongWritable, Text, LongWritable, Text, Text> {
    
    private int pid;
    
    private Querier querier;

    @Override
    public void setup(BSPPeer<LongWritable, Text, LongWritable, Text, Text> peer) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(peer);
        pid=peer.getPeerIndex();
    }

    @Override
    public void cleanup(BSPPeer<LongWritable, Text, LongWritable, Text, Text> peer) throws IOException {
        // TODO Auto-generated method stub
        super.cleanup(peer);
    }

    @Override
    public void bsp(BSPPeer<LongWritable, Text, LongWritable, Text, Text> peer) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub

        buildPartialTree(peer);

    }

    private void buildPartialTree(BSPPeer<LongWritable, Text, LongWritable, Text, Text> peer)
            throws IOException, SyncException, InterruptedException {
        querier=new Querier(PartialTreeBuilder.buildPartialTree(pid, peer));
    }

}
