package cn.esau.hamalxq.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Axis;
import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.entry.Step;
import cn.esau.hamalxq.utils.Utils;

public class Querier {

    private PartialTree pt;

    private BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer;

    private Map<String, Step> xpathMap;

    private int taskNum = 0;

    private Communication com;

    private List<List<Node>> resultLists;

    public Querier() throws IOException, SyncException, InterruptedException {
        super();
    }

    public void start() throws IOException, SyncException, InterruptedException {

        com = new Communication(peer);

        for (String key : xpathMap.keySet()) {
            Step xpath = xpathMap.get(key);

            query(xpath);

            sync();
        }

    }

    private List<Node> query(Step xpath) throws IOException, SyncException, InterruptedException {

        com.sendNode(0, pt.getRoot());
        sync();

        if (isMarster()) {
            resultLists = com.receiveFromAllPeer();
        }
        sync();

        Step step = xpath;
        while (step != null) {
            if (isMarster()) {
                com.sendNodeLists(resultLists);
            }
            sync();
            List<Node> inputList = com.receiveNodeList();
            queryWithAixs(step.getAxis(), inputList, step.getNameTest());
            

             step=step.getNext();
//            break;
        }

        return null;
    }

    public void queryWithAixs(Axis axis, List<Node> inputList, String test)
            throws IOException, SyncException, InterruptedException {
        
        // Child axis
        if (Axis.CHILD.equals(axis)) {
            queryChid(inputList, test);
        }

        // Descendant axis
        if (Axis.DESCENDANT.equals(axis)) {
             queryDescendant(inputList, test);
        }

        // Parent axis
        if (Axis.PARENT.equals(axis)) {
            // return queryParent(inputLists, test);
        }

        // Following-sibling axis
        if (Axis.FOLLOWING_SIBLING.equals(axis)) {
            // return queryFollowingSibling(inputLists, test);
        }

    }

    private void queryChid(List<Node> inputList, String test) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub
        
        Utils.printNods(pt.getPid(), inputList);
        
        com.sendNodeList(0, pt.findChildNodes(inputList, test));
        
        sync();
        
        if(isMarster()) {
            resultLists=com.receiveFromAllPeer();
            Utils.print(resultLists);
        }
        
        sync();
        
    }

    private void queryDescendant(List<Node> inputList, String test) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub
        
        Utils.printNods(pt.getPid(), inputList);
        
        com.sendNodeList(0, pt.findChildNodes(inputList, test));
        
        sync();
        
        if(isMarster()) {
            resultLists=com.receiveFromAllPeer();
            Utils.print(resultLists);
        }
        
        sync();
        
    }

    private void sync() throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub
        peer.sync();
    }

    private boolean isMarster() {
        return peer.getPeerIndex() == 0;
    }

    public BSPPeer<LongWritable, Text, LongWritable, Text, Node> getPeer() {
        return peer;
    }

    public void setPeer(BSPPeer<LongWritable, Text, LongWritable, Text, Node> peer) {
        this.peer = peer;
        taskNum = peer.getNumPeers();
    }

    public Map<String, Step> getXpathMap() {
        return xpathMap;
    }

    public void setXpathMap(Map<String, Step> xpathMap) {
        this.xpathMap = xpathMap;
    }

    public PartialTree getPt() {
        return pt;
    }

    public void setPt(PartialTree pt) {
        this.pt = pt;
    }

}
