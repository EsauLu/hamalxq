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

import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.NodeType;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.entry.Tag;
import cn.esau.hamalxq.entry.TagType;
import cn.esau.hamalxq.factory.NodeFactory;
import cn.esau.hamalxq.utils.Utils;

public class PartialTreeBuilder {

    public static PartialTree buildPartialTree(int pid, BSPPeer<LongWritable, Text, LongWritable, Text, Text> peer)
            throws IOException, SyncException, InterruptedException {
        
        List<Node> subTrees=null;
        System.out.println("Build SubTrees...");
        subTrees = buildSubTrees(peer);
        
        for(Node root: subTrees) {
            Utils.bfsWithDepth(pid, root);
        }
        
        System.out.println("Select Left Open Nodes...");
        List<Node> ll = selectLeftOpenNodes(subTrees);
        
        for(Node node: ll) {
            peer.send(peer.getPeerName(0), new Text(node.toText()));
        }
        
        peer.sync();
        
        if(peer.getPeerIndex()==0) {
            int num=peer.getNumCurrentMessages();
            System.out.println("Messege : "+num);
        }

        return null;
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


    private static List<Node> selectRighttOpenNodes(List<Node> subTrees) {

        List<Node> rl = new ArrayList<>();

        if (subTrees != null && subTrees.size() > 0) {
            Node node = subTrees.get(subTrees.size()-1);
            while (node != null && node.isRightOpenNode()) {
                rl.add(node);
                node = node.getLastChild();
            }
        }

        return rl;
    }

    public static List<Node> buildSubTrees(BSPPeer<LongWritable, Text, LongWritable, Text, Text> peer)
            throws IOException, SyncException, InterruptedException {

        Deque<Node> stack = new ArrayDeque<Node>();
        stack.push(NodeFactory.createNode("ROOT", NodeType.CLOSED_NODE, 0));

        LongWritable key = new LongWritable();
        Text value = new Text();

        while (peer.readNext(key, value)) {

            Tag tag = getTag(value.toString().trim());

            if (tag != null) {

                // System.out.println(tag.toString());

                if (TagType.START.equals(tag.getType())) {

                    Node node = NodeFactory.createNode(tag.getName(), NodeType.CLOSED_NODE, tag.getTid());
                    stack.push(node);

                } else {

                    if (TagType.FULL.equals(tag.getType())) {
                        Node node = NodeFactory.createNode(tag.getName(), NodeType.CLOSED_NODE, tag.getTid());
                        stack.push(node);
                    }

                    Node node = stack.peek();

                    if (node.getTagName().equals(tag.getName())) {
                        stack.pop();
                        stack.peek().addLastChild(node);
                    } else {
                        Node temNode = NodeFactory.createNode(tag.getName(), NodeType.LEFT_OPEN_NODE, tag.getTid());
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
        
//        int index = tagStr.indexOf(" ");
//        String tidStr = tagStr.substring(0, index);
//        tagStr = tagStr.substring(index + 1);

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

//        try {
//            tag.setTid(Integer.parseInt(tidStr));
//        } catch (NumberFormatException e) {
//            // TODO Auto-generated catch block
//            e.printStackTrace();
//        }

        return tag;

    }

}
