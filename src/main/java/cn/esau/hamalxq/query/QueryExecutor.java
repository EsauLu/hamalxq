package cn.esau.hamalxq.query;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Axis;
import cn.esau.hamalxq.entry.Link;
import cn.esau.hamalxq.entry.Message;
import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.PNode;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.entry.Step;
import cn.esau.hamalxq.utils.MsgMannager;

public class QueryExecutor {

    private MsgMannager msgMannager = new MsgMannager();

    public void multiQuery(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, Map<String, Step> xpaths)
            throws IOException, SyncException, InterruptedException {

        try {
            for (String key : xpaths.keySet()) {
                Step xpath = xpaths.get(key);
                peer.sync();
                long t1 = System.currentTimeMillis();
                List<Node> result = query(peer, pt, xpath);
                long t2 = System.currentTimeMillis();

//                printResult(peer, key, xpath, result, t1);

//                writeResult(peer, xpath, result, t2 - t1);
                
                writeResultData(peer, key, xpath, result, t1);
                System.gc();

            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
    
    private void writeResultData(BSPPeer<LongWritable, Text, Text, Text, Message> peer, String key, 
            Step xpath, List<Node> result, long t1)
            throws IOException, SyncException, InterruptedException {
        long count=0;
        for(Node node: result) {
            if(node.isLeftOpenNode()||node.isClosedNode()) {
                count++;
            }
        }
        PNode tem=new PNode(new Node(result.size()), new Link(peer.getPeerIndex(), count));
        msgMannager.sendPNode(peer, 0, tem);
        peer.sync();
        if(isMaster(peer)) {
            List<PNode> res=msgMannager.receivePNodeList(peer);
            long t2=System.currentTimeMillis();
            peer.write(new Text(""), new Text(""));
            peer.write(new Text(key+" : "), new Text(xpath.toXPath()));
            peer.write(new Text(""), new Text(""));
            peer.write(new Text("Number of nodes : "), new Text(""));
            peer.write(new Text(""), new Text(""));
            Map<Integer, PNode> resMap=new HashMap<>();
            for(PNode pNode: res) {
                resMap.put(pNode.getLink().getPid(), pNode);
            }
            count=0;
            for(int i=0; i<peer.getNumPeers(); i++) {
                PNode pNode=resMap.get(i);
                peer.write(new Text("\tpt"+i+" : "), new Text(""+pNode.getNode().getUid()));
                count+=pNode.getLink().getUid();
            }
            peer.write(new Text(""), new Text(""));
            peer.write(new Text("Number of all nodes : "), new Text(""+count));
            peer.write(new Text(""), new Text(""));
            peer.write(new Text("Time out : "), new Text((t2-t1)+" ms"));
            peer.write(new Text(""), new Text(""));
            peer.write(new Text("-------------------------------------------------------------------------"), new Text(""));
        }
        peer.sync();


        
    }

    private void printResult(BSPPeer<LongWritable, Text, Text, Text, Message> peer, String key,
            Step xpath, List<Node> result, long beginTime)
            throws IOException, SyncException, InterruptedException {

        msgMannager.sendNodeList(peer, 0, result);

        peer.sync();

        if (isMaster(peer)) {
            List<List<Node>> results = msgMannager.receiveNodesFromAllPeer(peer);
            long endTime = System.currentTimeMillis();
            StringBuilder sb = new StringBuilder();

            sb.append("\n");
            sb.append(key+" : " + xpath.toXPath());
            sb.append("\n\n");

            long count = 0;
            for (int i = 0; i < results.size(); i++) {
                sb.append("\tpt" + i + " : ");
                List<Node> res = results.get(i);
                count += res.size();
                for (Node node : res) {
                    sb.append(node);
                }
                sb.append("\n");
            }
            sb.append("\n\n");

            sb.append("Num : " + count);
            sb.append("\n\n");
            sb.append("Time out : " + (endTime - beginTime) + " ms");
            sb.append("\n\n");
            sb.append("-----------------------------------------------------------------\n");
            System.out.println(sb.toString());

        }

        peer.sync();

    }

    private List<Node> query(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, Step xpath)
            throws IOException, SyncException, InterruptedException {
        List<Node> resultList = new ArrayList<Node>();
        resultList.add(pt.getRoot());
        Step step = xpath;
        while (step != null) {
            resultList = queryByAixs(peer, pt, resultList, step);
            Step predicate = step.getPredicate();
            if (predicate != null) {
                List<PNode> pResultList = preparePredicate(peer, resultList);
                pResultList = predicateQuery(peer, pt, pResultList, predicate);
                resultList = processPredicate(peer, pt, pResultList);
            }
            step = step.getNext();
            peer.sync();
        }
        return resultList;
    }

    private List<PNode> predicateQuery(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<PNode> inputList, Step predicate)
            throws IOException, SyncException, InterruptedException {
        List<PNode> resultList = inputList;
        Step step = predicate;
        while (step != null) {
            resultList = predicateQueryByAixs(peer, pt, resultList, step);
            Step predicate2 = step.getPredicate();
            if (predicate2 != null) {
                List<PNode> pResultList = rePreparePredicate(peer.getPeerIndex(), resultList);
                pResultList = predicateQuery(peer, pt, pResultList, predicate2);
                resultList = filter(peer, pt, resultList, pResultList);
            }
            step = step.getNext();
            peer.sync();
        }
        return resultList;
    }

    public List<Node> queryByAixs(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<Node> inputList, Step step)
            throws IOException, SyncException, InterruptedException {
        Axis axis = step.getAxis();
        String test = step.getNameTest();
        if (Axis.CHILD.equals(axis)) {
            return queryChild(pt, inputList, test);
        }
        if (Axis.DESCENDANT.equals(axis)) {
            return queryDescendant(pt, inputList, test);
        }
        if (Axis.PARENT.equals(axis)) {
            return queryParent(peer, pt, inputList, test);
        }
        if (Axis.FOLLOWING_SIBLING.equals(axis)) {
            return queryFollowingSibling(peer, pt, inputList, test);
        }

        // 其他的Axis...

        return null;
    }

    public List<PNode> predicateQueryByAixs(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<PNode> inputList, Step step)
            throws IOException, SyncException, InterruptedException {
        Axis axis = step.getAxis();
        String test = step.getNameTest();
        if (Axis.CHILD.equals(axis)) {
            return queryPredicateChild(pt, inputList, test);
        }
        if (Axis.DESCENDANT.equals(axis)) {
            return queryPredicateDescendant(pt, inputList, test);
        }
        if (Axis.PARENT.equals(axis)) {
            return queryPredicateParent(peer, pt, inputList, test);
        }
        if (Axis.FOLLOWING_SIBLING.equals(axis)) {
            return queryPredicateFolSib(peer, pt, inputList, test);
        }

        // 其他的Axis...

        return new ArrayList<>();
    }

    private List<Node> queryChild(PartialTree pt, List<Node> inputList, String test) throws IOException, SyncException, InterruptedException {
        List<Node> outputList = new ArrayList<>();
        for (Node node : inputList) {
            node = pt.findNodeByUid(node.getUid());
            for (int j = 0; j < node.getChildNum(); j++) {
                Node ch = node.getChildByIndex(j);
                if (checkNameTest(test, ch)) {
                    outputList.add(ch);
                }
            }
        }
        return outputList;
    }

    private List<PNode> queryPredicateChild(PartialTree pt, List<PNode> inputList, String test)
            throws IOException, SyncException, InterruptedException {
        List<PNode> outputList = new ArrayList<>();
        for (PNode pnode : inputList) {
            Node node = pt.findNodeByUid(pnode.getNode().getUid());
            for (int j = 0; j < node.getChildNum(); j++) {
                Node ch = node.getChildByIndex(j);
                if (checkNameTest(test, ch)) {
                    outputList.add(new PNode(ch, pnode.getLink()));
                }
            }
        }
        return outputList;
    }

    private List<Node> queryDescendant(PartialTree pt, List<Node> inputList, String test) throws IOException, SyncException, InterruptedException {
        List<Node> outputList = new ArrayList<>();
        setIsChecked(pt, false);
        for (Node node : inputList) {
            node = pt.findNodeByUid(node.getUid());
            if (node != null) {
                Deque<Node> stack = new ArrayDeque<>();
                stack.push(node);
                while (!stack.isEmpty()) {
                    Node nt = stack.pop();
                    if (nt.isChecked()) {
                        continue;
                    }
                    nt.setChecked(true);
                    for (int j = 0; j < nt.getChildNum(); j++) {
                        Node ch = nt.getChildByIndex(j);
                        if (checkNameTest(test, ch)) {
                            outputList.add(ch);
                        }
                        stack.push(ch);
                    }
                }
            }
        }
        return outputList;
    }

    private List<PNode> queryPredicateDescendant(PartialTree pt, List<PNode> inputList, String test)
            throws IOException, SyncException, InterruptedException {
        List<PNode> outputList = new ArrayList<>();
        for (PNode pnode : inputList) {
            Node node = pt.findNodeByUid(pnode.getNode().getUid());
            if (node != null) {
                Deque<Node> stack = new ArrayDeque<>();
                stack.push(node);
                while (!stack.isEmpty()) {
                    Node nt = stack.pop();
                    for (int j = 0; j < nt.getChildNum(); j++) {
                        Node ch = nt.getChildByIndex(j);
                        if (checkNameTest(test, ch)) {
                            outputList.add(new PNode(ch, pnode.getLink()));
                        }
                        stack.push(ch);
                    }
                }
            }
        }
        return outputList;
    }

    private List<Node> queryParent(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<Node> inputList, String test)
            throws IOException, SyncException, InterruptedException {
        List<Node> outputList = new ArrayList<>();
        for (Node node : inputList) {
            node = pt.findNodeByUid(node.getUid());
            if (node == null) {
                continue;
            }
            Node parent = node.getParent();
            if (parent != null && checkNameTest(test, parent)) {
                outputList.add(parent);
            }
        }
        return shareNodes(peer, pt, outputList);
    }

    private List<PNode> queryPredicateParent(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<PNode> inputList,
            String test) throws IOException, SyncException, InterruptedException {
        List<PNode> outputList = new ArrayList<>();
        for (PNode pnode : inputList) {
            Node node = pt.findNodeByUid(pnode.getNode().getUid());
            if (node == null) {
                continue;
            }
            Node parent = node.getParent();
            if (parent != null && checkNameTest(test, parent)) {
                outputList.add(new PNode(parent, pnode.getLink()));
            }
        }
        return sharePNodes(peer, pt, outputList);
    }

    private List<Node> shareNodes(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<Node> inputList)
            throws IOException, SyncException, InterruptedException {
        for (Node node : inputList) {
            if (!node.isClosedNode()) {
                for (int i = node.getStart(); i <= node.getEnd(); i++) {
                    msgMannager.sendNode(peer, i, node);
                }
            }
        }
        peer.sync();
        List<Node> toBeShared = msgMannager.receiveNodeList(peer);
        peer.sync();
        Set<Node> set = new TreeSet<>(inputList);
        for (Node node : toBeShared) {
            node = pt.findNodeByUid(node.getUid());
            if (!set.contains(node)) {
                set.add(node);
            }
        }
        return new ArrayList<>(set);
    }

    private List<PNode> sharePNodes(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<PNode> inputList)
            throws IOException, SyncException, InterruptedException {
        for (PNode pnode : inputList) {
            Node node = pnode.getNode();
            if (!node.isClosedNode()) {
                for (int i = node.getStart(); i <= node.getEnd(); i++) {
                    msgMannager.sendPNode(peer, i, pnode);
                }
            }
        }
        peer.sync();
        List<PNode> toBeShared = msgMannager.receivePNodeList(peer);
        peer.sync();
        Set<Node> set = new TreeSet<>();
        for (PNode pNode : inputList) {
            set.add(pNode.getNode());
        }
        for (PNode pnode : toBeShared) {
            Node node = pt.findNodeByUid(pnode.getNode().getUid());
            if (!set.contains(node)) {
                inputList.add(new PNode(node, pnode.getLink()));
            }
        }
        return inputList;
    }

    private List<Node> queryFollowingSibling(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<Node> inputList, String test)
            throws IOException, SyncException, InterruptedException {
        // 本地查询
        setIsChecked(pt, false);
        List<Node> localResult = new ArrayList<Node>();
        for (Node node : inputList) {
            node = pt.findNodeByUid(node.getUid());
            while (!node.isChecked() && node.getFolsib() != null) {
                node.setChecked(true);
                node = node.getFolsib();
                if (checkNameTest(test, node)) {
                    localResult.add(node);
                }
            }
        }

        // 发送需要远程查询的结点
        for (Node node : inputList) {
            Node parent = pt.findNodeByUid(node.getUid()).getParent();
            if ((node.isLeftOpenNode() || node.isClosedNode()) && parent != null && (parent.isRightOpenNode() || parent.isPreOpenNode())) {
                for (int i = peer.getPeerIndex() + 1; i <= parent.getEnd(); i++) {
                    msgMannager.sendNode(peer, i, parent);
                }
            }
        }
        peer.sync();

        // 远程查询
        List<Node> remoteInputList = msgMannager.receiveNodeList(peer);
        peer.sync();
        List<Node> remoteResult = pt.findChildNodes(remoteInputList, test);

        return mergeNodeList(localResult, remoteResult);
    }

    private List<PNode> queryPredicateFolSib(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<PNode> inputList,
            String test) throws IOException, SyncException, InterruptedException {
        // 本地查询
        List<PNode> localResult = new ArrayList<PNode>();
        for (PNode pnode : inputList) {
            Node node = pt.findNodeByUid(pnode.getNode().getUid());
            while (node.getFolsib() != null) {
                node = node.getFolsib();
                if (checkNameTest(test, node)) {
                    localResult.add(new PNode(node, pnode.getLink()));
                }
            }
        }

        // 发送需要远程查询的结点
        for (PNode pnode : inputList) {
            Node node = pnode.getNode();
            Node parent = pt.findNodeByUid(node.getUid()).getParent();
            if ((node.isLeftOpenNode() || node.isClosedNode()) && parent != null && (parent.isRightOpenNode() || parent.isPreOpenNode())) {
                for (int i = peer.getPeerIndex() + 1; i <= parent.getEnd(); i++) {
                    msgMannager.sendPNode(peer, i, new PNode(parent, pnode.getLink()));
                }
            }
        }
        peer.sync();

        // 远程查询
        List<PNode> remoteInputList = msgMannager.receivePNodeList(peer);
        peer.sync();
        List<PNode> remoteResult = pt.findChildPNodes(remoteInputList, test);

        return mergePNodeList(localResult, remoteResult);
    }

    private List<Node> mergeNodeList(List<Node> list1, List<Node> list2) {
        Set<Node> set = new HashSet<>(list1);
        set.addAll(list2);
        return new ArrayList<>(set);
    }

    private List<PNode> mergePNodeList(List<PNode> list1, List<PNode> list2) {
        Set<PNode> set = new HashSet<>(list1);
        set.addAll(list2);
        return new ArrayList<>(set);
    }

    public List<PNode> preparePredicate(BSPPeer<LongWritable, Text, Text, Text, Message> peer, List<Node> inputList) {
        List<PNode> outputList = new ArrayList<PNode>();
        for (Node node : inputList) {
            outputList.add(new PNode(node, new Link(peer.getPeerIndex(), node.getUid())));
        }
        return outputList;
    }

    private List<Node> processPredicate(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, List<PNode> inputList)
            throws IOException, SyncException, InterruptedException {
        for (PNode pnode : inputList) {
            Link link = pnode.getLink();
            msgMannager.sendNode(peer, link.getPid(), new Node(link.getUid()));
        }
        peer.sync();
        List<Node> activedNodes = msgMannager.receiveNodeList(peer);
        peer.sync();
        List<Node> outputList = new ArrayList<Node>();
        for (Node node : activedNodes) {
            outputList.add(pt.findNodeByUid(node.getUid()));
        }
        return shareNodes(peer, pt, outputList);
    }

    private List<PNode> rePreparePredicate(int pid, List<PNode> inputList) {
        List<PNode> outputList = new ArrayList<>();
        Set<Node> nodes = new HashSet<>();
        for (PNode pnode : inputList) {
            nodes.add(pnode.getNode());
        }
        for (Node node : nodes) {
            outputList.add(new PNode(node, new Link(pid, node.getUid())));
        }
        return outputList;
    }

    private List<PNode> filter(BSPPeer<LongWritable, Text, Text, Text, Message> peer, PartialTree pt, 
            List<PNode> list1, List<PNode> list2)
            throws IOException, SyncException, InterruptedException {        
        for(PNode pNode: list2) {
            Link link=pNode.getLink();
            msgMannager.sendNode(peer, link.getPid(), new Node(link.getUid()));
        }
        peer.sync();
        List<Node> predicateResult=msgMannager.receiveNodeList(peer);
        peer.sync();        
        Map<Node, List<Link>> linksMap=new HashMap<>();
        for(PNode pNode:list1) {
            Node node=pNode.getNode();
            Link link=pNode.getLink();
            List<Link> links=linksMap.get(node);
            if(links==null) {
                links=new ArrayList<>();
                linksMap.put(node, links);
            }
            links.add(link);
        }
        List<PNode> outputList=new ArrayList<>();        
        for(Node node: predicateResult) {
            node=pt.findNodeByUid(node.getUid());
            List<Link> links=linksMap.get(node);
            if(links!=null) {
                for(Link link: links) {
                    outputList.add(new PNode(node, link));
                }
            }
        }        
        return outputList;
    }

    private void setIsChecked(PartialTree pt, boolean check) {
        pt.setIsChecked(check);
    }

    public boolean checkNameTest(String test, Node node) {
        if (test.trim().equals("*")) {
            return true;
        }
        return node.getTagName().equals(test.trim());
    }

    private boolean isMaster(BSPPeer<LongWritable, Text, Text, Text, Message> peer) throws IOException, SyncException, InterruptedException {
        // TODO Auto-generated method stub

        return peer.getPeerIndex() == 0;

    }
}
