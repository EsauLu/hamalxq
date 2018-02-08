package cn.esau.hamalxq.factory;

import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.NodeType;

public class NodeFactory {

    public static Node createNode(String tagName, NodeType type, int pid) {
        Node node = new Node();
        node.setTagName(tagName);
        node.setType(type);
        node.setUid(Long.MIN_VALUE);
        node.setStart(pid);
        node.setEnd(pid);
        node.setPid(pid);
        return node;
    }

}
