package cn.esau.hamalxq.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Link;
import cn.esau.hamalxq.entry.Message;
import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.PNode;

public class Communication {

    private BSPPeer<LongWritable, Text, Text, Text, Message> peer;
    
    private int taskNum;

    public Communication(BSPPeer<LongWritable, Text, Text, Text, Message> peer) {
        // TODO Auto-generated constructor stub
        this.peer = peer;
        this.taskNum=peer.getNumPeers();
    }
    
    public void sendNode(int peerIndex, Node node) throws IOException, SyncException, InterruptedException {
        Message msg=new Message(node, null);
    	peer.send(peer.getPeerName(peerIndex), msg);
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
    
    public List<List<Node>> receiveNodesFromAllPeer() throws IOException, SyncException, InterruptedException {
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
    
    public List<Node> receiveNodeList() throws IOException, SyncException, InterruptedException {
        List<Node> list=new ArrayList<>();
        
        while(true) {
        	Message message=peer.getCurrentMessage();
            if(message==null) {
                break;
            }
            Node node=message.getNode();
            list.add(node);
        }
        
        return list;
    }
    
    
    
    
    


    
    public void sendPNode(int peerIndex, PNode pNode) throws IOException, SyncException, InterruptedException {
        Message msg=new Message(pNode.getNode(), pNode.getLink());
    	peer.send(peer.getPeerName(peerIndex), msg);
    }

    public void sendPNodeList(int peerIndex, List<PNode> list) throws IOException, SyncException, InterruptedException {
        for(PNode pnode: list) {
            sendPNode(peerIndex, pnode);
        }
    }

    public void sendPNodeLists(List<List<PNode>> lists) throws IOException, SyncException, InterruptedException {
        for(int i=0;i<lists.size();i++) {
            List<PNode> list = lists.get(i);
            sendPNodeList(i, list);
        }
    }
    
    public void sendPNodesToAllWorker(List<PNode> list) throws IOException, SyncException, InterruptedException {
    	for(int i=0;i<taskNum;i++) {
    		sendPNodeList(i, list);
    	}
    }
    
    public List<List<PNode>> receivePNodesFromAllPeer() throws IOException, SyncException, InterruptedException {
        List<List<PNode>> resultLists=new ArrayList<>(taskNum);
        for(int i=0;i<taskNum;i++) {
            resultLists.add(new ArrayList<PNode>());
        }
        
        while(true) {
        	Message message=peer.getCurrentMessage();
            if(message==null) {
                break;
            }
            Node node=message.getNode();
            Link link=message.getLink();
            PNode pNode=new PNode(node, link);
            resultLists.get(node.getPid()).add(pNode);
        }
        
        return resultLists;
    }
    
    public List<PNode> receivePNodeList() throws IOException, SyncException, InterruptedException {
        List<PNode> list=new ArrayList<>();
        
        while(true) {
        	Message message=peer.getCurrentMessage();
            if(message==null) {
                break;
            }
            Node node=message.getNode();
            Link link=message.getLink();
            PNode pNode=new PNode(node, link);
            list.add(pNode);
        }
        
        return list;
    }

}



























