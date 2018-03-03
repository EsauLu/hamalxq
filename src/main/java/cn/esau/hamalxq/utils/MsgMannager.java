package cn.esau.hamalxq.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Message;
import cn.esau.hamalxq.entry.Node;

public class MsgMannager {

    public void sendNodeList(BSPPeer<LongWritable, Text, Text, Text, Message> peer, int peerIndex, List<Node> list)
            throws IOException, SyncException, InterruptedException {
        for (Node node : list) {
            sendNode(peer, peerIndex, node);
        }
    }

    public void sendNode(BSPPeer<LongWritable, Text, Text, Text, Message> peer, int peerIndex, Node node)
            throws IOException, SyncException, InterruptedException {
        Message msg = new Message(node, null);
        peer.send(peer.getPeerName(peerIndex), msg);
    }

    public Node receiveNode(BSPPeer<LongWritable, Text, Text, Text, Message> peer) throws IOException, SyncException, InterruptedException {
        Message msg = peer.getCurrentMessage();
        return msg == null ? null : msg.getNode();
    }

    public List<Node> receiveNodeList(BSPPeer<LongWritable, Text, Text, Text, Message> peer) throws IOException, SyncException, InterruptedException {
        List<Node> list = new ArrayList<>();

        while (true) {
            Message message = peer.getCurrentMessage();
            if (message == null) {
                break;
            }
            Node node = message.getNode();
            list.add(node);
        }

        return list;
    }

}
