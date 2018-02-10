package cn.esau.hamalxq.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.utils.Utils;

public class Communication {

    private BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer;
    
    private int taskNum;

    public Communication(BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer) {
        // TODO Auto-generated constructor stub
        this.peer = peer;
        this.taskNum=peer.getNumPeers();
    }
    
    public void sendNode(int peerIndex, Node node) throws IOException, SyncException, InterruptedException {
        peer.send(peer.getPeerName(peerIndex), node);
    }
    
    public void sendNodeList(int peerIndex, List<Node> list) throws IOException, SyncException, InterruptedException {
        for(Node node: list) {
            sendNode(peerIndex, node);
        }
    }
    
    public void sendNodeLists(List<List<Node>> lists) throws IOException, SyncException, InterruptedException {
        for(int i=0;i<lists.size();i++) {
            List<Node> list = lists.get(i);
            sendNodeList(i, list);
        }
    }
    
    public void sendNodesToAllWorker(List<Node> list) throws IOException, SyncException, InterruptedException {
    	for(int i=0;i<taskNum;i++) {
    		sendNodeList(i, list);
    	}
    }
    
    public List<List<Node>> receiveFromAllPeer() throws IOException, SyncException, InterruptedException {
        List<List<Node>> resultLists=new ArrayList<>(taskNum);
        for(int i=0;i<taskNum;i++) {
            resultLists.add(new ArrayList<Node>());
        }
        
        while(true) {
            Node node=peer.getCurrentMessage();
            if(node==null) {
                break;
            }
            resultLists.get(node.getPid()).add(node);
        }
        
        return resultLists;
    }
    
    public List<Node> receiveNodeList() throws IOException, SyncException, InterruptedException {
        List<Node> list=new ArrayList<>();
        
        while(true) {
            Node node=peer.getCurrentMessage();
            if(node==null) {
                break;
            }
            list.add(node);
        }
        
        return list;
    }

}



























