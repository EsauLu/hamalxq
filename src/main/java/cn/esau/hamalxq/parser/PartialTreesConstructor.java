package cn.esau.hamalxq.parser;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Message;
import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.NodeType;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.entry.Tag;
import cn.esau.hamalxq.entry.TagType;
import cn.esau.hamalxq.factory.NodeFactory;
import cn.esau.hamalxq.utils.MsgMannager;

public class PartialTreesConstructor {

    private MsgMannager msgMannager = new MsgMannager();
    
    private long nodeCount=0;

    public PartialTree buildPartialTree(BSPPeer<LongWritable, Text, Text, Text, Message> peer)
            throws IOException, SyncException, InterruptedException {
        long t1=System.currentTimeMillis();

        System.out.println("BuildSubTrees");
        List<Node> subTrees = buildSubTrees(peer);

        System.out.println("ComputePrePath");
        Node root = computePrePath(peer, subTrees);

        // Compute Uid numbers.
        System.out.println("Compute Uid numbers");
        int taskNum = peer.getNumPeers();
        for (int i = 0; i < taskNum; i++) {
            if (i == peer.getPeerIndex()) {
                computeUid(peer, root);
            }
            peer.sync();
        }

        System.out.println("Create Partiel-tree object");
        PartialTree pt = new PartialTree();
        pt.setRoot(root);
        pt.setPid(peer.getPeerIndex());
        pt.update();

        // Compute ranges.
        System.out.println("Compute ranges");
        computeRanges(peer, pt);
        
        long t2=System.currentTimeMillis();
        writeBuildResult(peer, pt, t2-t1);

        return pt;

    }
    
    private void writeBuildResult(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, long timeOut)
            throws IOException, SyncException, InterruptedException {
        peer.write(new Text(""), new Text(""));
        peer.write(new Text("Time out of build Partial-tree : "), new Text(timeOut+"ms"));
        peer.write(new Text(""), new Text(""));
        peer.write(new Text("========================================================="), new Text(""));
    }

    private void computeRanges(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt)
            throws IOException, SyncException, InterruptedException {
        for (int i = 0; i < peer.getNumPeers(); i++) {
            if (peer.getPeerIndex() == i) {
                List<Node> rangeNodes = msgMannager.receiveNodeList(peer);
                for (Node rangeNode : rangeNodes) {
                    Node node = pt.findNodeByUid(rangeNode.getUid());
                    node.setStart(rangeNode.getStart());
                }
                if (i + 1 < peer.getNumPeers()) {
                    msgMannager.sendNodeList(peer, i + 1, pt.getPreOpenNodes());
                    msgMannager.sendNodeList(peer, i + 1, pt.getRightOpenNodes());
                }
            }
            peer.sync();
        }
        for (int i = peer.getNumPeers()-1; i >= 0; i--) {
            if (peer.getPeerIndex() == i) {
                List<Node> rangeNodes = msgMannager.receiveNodeList(peer);
                for (Node rangeNode : rangeNodes) {
                    Node node = pt.findNodeByUid(rangeNode.getUid());
                    node.setEnd(rangeNode.getEnd());
                }
                if (i - 1 >= 0) {
                    msgMannager.sendNodeList(peer, i - 1, pt.getPreOpenNodes());
                    msgMannager.sendNodeList(peer, i - 1, pt.getLeftOpenNodes());
                }
            }
            peer.sync();
        }
    }

    private void computeUid(BSPPeer<LongWritable, Text, Text, Text, Message> peer, Node root)
            throws IOException, SyncException, InterruptedException {

        int pid = peer.getPeerIndex();
        Node uidStartNode = msgMannager.receiveNode(peer);
        long uid = -1;
        if (uidStartNode != null) {
            uid = uidStartNode.getUid();
        }

        Node p = root;
        List<Node> list = msgMannager.receiveNodeList(peer);
        for (Node node : list) {
            p.setUid(node.getUid());
            p = p.getFirstChild();
        }

        Deque<Node> stack = new ArrayDeque<Node>();
        stack.push(root);

        while (!stack.isEmpty()) {
            Node node = stack.pop();
            if (node.getUid() == Long.MIN_VALUE) {
                node.setUid(uid++);
            }
            for (int j = node.getChildNum() - 1; j >= 0; j--) {
                stack.push(node.getChildByIndex(j));
            }
        }

        if (pid + 1 < peer.getNumPeers()) {
            Node tem = new Node();
            tem.setUid(uid);
            msgMannager.sendNode(peer, pid + 1, tem);
            p = root;
            while (p != null && (p.isRightOpenNode() || p.isPreOpenNode())) {
                msgMannager.sendNode(peer, pid + 1, p);
                p = p.getLastChild();
            }
        }

    }

    public Node computePrePath(BSPPeer<LongWritable, Text, Text, Text, Message> peer, List<Node> subTrees)
            throws IOException, SyncException, InterruptedException {

        Node root = null;
        for (int i = 0; i < peer.getNumPeers(); i++) {
            if (peer.getPeerIndex() == i) {
                List<Node> prePathNode = msgMannager.receiveNodeList(peer);
                List<Node> leftList = selectLeftOpenNodes(subTrees);
                for (int j = 0; j < leftList.size(); j++) {
                    if (prePathNode.size() == 0) {
                        break;
                    }
                    prePathNode.remove(prePathNode.size() - 1);
                }
                if (i < peer.getNumPeers() - 1) {
                    msgMannager.sendNodeList(peer, i + 1, copyNodeList(prePathNode));
                    msgMannager.sendNodeList(peer, i + 1, copyNodeList(selectRightOpenNodes(subTrees)));
                }
                if (prePathNode.size() > 0) {
                    for (int j = 0; j < prePathNode.size() - 1; j++) {
                        Node node=prePathNode.get(j);
                        node.setType(NodeType.PRE_NODE);
                        node.addLastChild(prePathNode.get(j + 1));
                    }
                    Node preNode = prePathNode.get(prePathNode.size() - 1);
                    preNode.setType(NodeType.PRE_NODE);
                    for (Node node : subTrees) {
                        preNode.addLastChild(node);
                    }
                    root = prePathNode.get(0);
                } else {
                    root = subTrees.get(0);
                }
            }
            peer.sync();
        }

        return root;
    }

    private static List<Node> selectLeftOpenNodes(List<Node> subTrees) {

        List<Node> ll = new ArrayList<>();

        if (subTrees != null && subTrees.size() > 0) {
            Node node = subTrees.get(0);
            while (node != null && node.isLeftOpenNode()) {
                ll.add(node);
                node = node.getFirstChild();
            }
        }

        return ll;
    }

    private static List<Node> selectRightOpenNodes(List<Node> subTrees) {

        List<Node> rl = new ArrayList<>();

        if (subTrees != null && subTrees.size() > 0) {
            Node node = subTrees.get(subTrees.size() - 1);
            while (node != null && node.isRightOpenNode()) {
                rl.add(node);
                node = node.getLastChild();
            }
        }

        return rl;
    }

    public List<Node> buildSubTrees(BSPPeer<LongWritable, Text, Text, Text, Message> peer) throws IOException, SyncException, InterruptedException {

        nodeCount=0;
        int pid = peer.getPeerIndex();
        Deque<Node> stack = new ArrayDeque<Node>();
        stack.push(NodeFactory.createNode("?", NodeType.CLOSED_NODE, pid));

        LongWritable key = new LongWritable();
        Text value = new Text();

        int peerNum = peer.getNumPeers();
        int peerIndex = peer.getPeerIndex();
        boolean first = true;
        boolean last = false;
        boolean res = false;
        while (true) {

            if (peerIndex == 0 && first == true) {
                value.set("ROOT");
                first = false;
            } else {
                res = peer.readNext(key, value);
                if (res == false) {
                    if (peerIndex == peerNum - 1 && last == false) {
                        value.set("/ROOT");
                        last = true;
                    } else {
                        break;
                    }
                }
            }

            Tag tag = getTag(value.toString().trim());

            if (tag != null) {

                // System.out.println(tag.toString());

                if (TagType.START.equals(tag.getType())) {
                    nodeCount++;
                    Node node = NodeFactory.createNode(tag.getName(), NodeType.CLOSED_NODE, pid);
                    stack.push(node);

                } else {

                    if (TagType.FULL.equals(tag.getType())) {
                        nodeCount++;
                        Node node = NodeFactory.createNode(tag.getName(), NodeType.CLOSED_NODE, pid);
                        stack.push(node);
                    }

                    Node node = stack.peek();

                    if (node.getTagName().equals(tag.getName())) {
                        stack.pop();
                        stack.peek().addLastChild(node);
                    } else {
                        nodeCount++;
                        Node temNode = NodeFactory.createNode(tag.getName(), NodeType.LEFT_OPEN_NODE, pid);
                        temNode.addChilds(node.getAllChilds());
                        node.clearChilds();
                        node.addLastChild(temNode);
                    }

                }

            }

        }

        while (stack.size() > 1) {
            Node node = stack.pop();
            node.setType(NodeType.RIGHT_OPEN_NODE);
            stack.peek().addLastChild(node);
        }

        return stack.pop().getAllChilds();

    }

    private static Tag getTag(String tagStr) {

        if (tagStr == null || tagStr.trim().equals("")) {
            return null;
        }

        Tag tag = new Tag();

        tagStr = tagStr.trim();
        char ch = tagStr.charAt(0);
        if (ch == '/') {
            tagStr = tagStr.substring(1);
            tag.setType(TagType.END);
        } else if (tagStr.charAt(tagStr.length() - 1) == '/') {
            tagStr = tagStr.substring(0, tagStr.length() - 1);
            tag.setType(TagType.FULL);
        } else if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')) {
            tag.setType(TagType.START);
        } else {
            return null;
        }

        int i = tagStr.indexOf(" ");
        if (i != -1) {
            tag.setName(tagStr.substring(0, i));
        } else {
            tag.setName(tagStr);
        }

        return tag;

    }
    
    private List<Node> copyNodeList(List<Node> list){
        List<Node> cp=new ArrayList<>();
        for(Node node: list) {
            cp.add(node.copy());
        }
        return cp;
    }

}
