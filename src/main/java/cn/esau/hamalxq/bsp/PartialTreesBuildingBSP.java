package cn.esau.hamalxq.bsp;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSP;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.NodeType;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.parser.PartialTreeBuilder;
import cn.esau.hamalxq.query.Querier;

public class PartialTreesBuildingBSP extends BSP<LongWritable, Text, LongWritable, Text, Node> {

    private int pid;

    private Querier querier;

    @Override
    public void setup(BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub
        super.setup(peer);
        pid = peer.getPeerIndex();
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
        
        long c=0;

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
        querier = new Querier(PartialTreeBuilder.buildPartialTree(pid, peer));
    }

}
