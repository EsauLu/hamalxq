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
import cn.esau.hamalxq.entry.PNode;

public class MsgMannager {

    public void sendPNodeList(BSPPeer<LongWritable, Text, Text, Text, Message> peer, int peerIndex, List<PNode> plist)
            throws IOException, SyncException, InterruptedException {
        for (PNode pnode : plist) {
            sendPNode(peer, peerIndex, pnode);
        }
    }

    public void sendPNode(BSPPeer<LongWritable, Text, Text, Text, Message> peer, int peerIndex, PNode pnode)
            throws IOException, SyncException, InterruptedException {
        Message msg = new Message(pnode.getNode(), pnode.getLink());
        peer.send(peer.getPeerName(peerIndex), msg);
    }

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
    
    public List<List<Node>> receiveNodesFromAllPeer(BSPPeer<LongWritable, Text, Text, Text, Message> peer) 
            throws IOException, SyncException, InterruptedException {
        int taskNum=peer.getNumPeers();
        List<List<Node>> resultLists=new ArrayList<>(taskNum);
        for(int i=0;i<taskNum;i++) {
            resultLists.add(new ArrayList<Node>());
        }
        
        while(true) {
            Message message=peer.getCurrentMessage();
            if(message==null) {
                break;
            }
            Node node=message.getNode();
            resultLists.get(node.getPid()).add(node);
        }
        
        return resultLists;
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

    public List<PNode> receivePNodeList(BSPPeer<LongWritable, Text, Text, Text, Message> peer) throws IOException, SyncException, InterruptedException {
        List<PNode> list = new ArrayList<>();

        while (true) {
            Message message = peer.getCurrentMessage();
            if (message == null) {
                break;
            }
            list.add(new PNode(message.getNode(), message.getLink()));
        }

        return list;
    }

}
