package cn.esau.hamalxq.parser;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import cn.esau.hamalxq.query.Communication;

public class PartialTreeBuilder {

	private Communication com;

	private BSPPeer<LongWritable, Text, Text, Text, Message> peer;

	private int taskNum = 0;

	private int pid = 0;

	public PartialTreeBuilder(Communication com, BSPPeer<LongWritable, Text, Text, Text, Message> peer) {
		super();
		this.com = com;
		this.peer = peer;
		this.taskNum = peer.getNumPeers();
		this.pid = peer.getPeerIndex();
	}

	public PartialTree buildPartialTree() throws IOException, SyncException, InterruptedException {

		PartialTree pt=null;
		try {
			List<Node> subTrees = null;
			subTrees = buildSubTrees();

			// GetPrePath
			Node root = getPrePath(subTrees);

			// Compute Uid numbers.
			int taskNum = peer.getNumPeers();
			for (int i = 0; i < taskNum; i++) {
				if (i == peer.getPeerIndex()) {
					computeUid(root);
				}
				peer.sync();
			}

			pt = new PartialTree();
			pt.setRoot(root);
			pt.setPid(pid);
			pt.update();

			// Compute ranges.
			computeRanges(pt);

			peer.sync();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Utils.bfsWithRanges(pid, root);

		return pt;
	}

	private Node getPrePath(List<Node> subTrees) throws IOException, SyncException, InterruptedException {

		List<Node> ll = selectLeftOpenNodes(subTrees);
		 com.sendNodeList(0, ll);
//		for (Node node : ll) {
//			Node tem = NodeFactory.createNode(node.getTagName(), node.getType(), node.getPid());
//			com.sendNode(0, tem);
//			// peer.send(peer.getPeerName(0), NodeFactory.createNode(node.getTagName(),
//			// node.getType(), node.getPid()));
//		}

		List<Node> rl = selectRightOpenNodes(subTrees);
		 com.sendNodeList(0, rl);
//		for (Node node : rl) {
//			Node tem = NodeFactory.createNode(node.getTagName(), node.getType(), node.getPid());
//			com.sendNode(0, tem);
//			// peer.send(peer.getPeerName(0), NodeFactory.createNode(node.getTagName(),
//			// node.getType(), node.getPid()));
//		}

		peer.sync();

		if (isMaster()) {
			computPrePath(subTrees);
		}

		peer.sync();

		Node root = addPrePath(subTrees);

		return root;

	}

	private boolean isMaster() {
		// TODO Auto-generated method stub
		return pid == 0;
	}

	private void computeRanges(PartialTree pt) throws IOException, SyncException, InterruptedException {
		
		List<Node> openNodes = selectOpenNodes(pt);

		com.sendNodeList(0, openNodes);

		peer.sync();

		if (isMaster()) {
			List<Node> ranges = prepareRanges(pt);
			com.sendNodesToAllWorker(ranges);
		}

		peer.sync();

		List<Node> rangsNodes = com.receiveNodeList();

		setRangs(pt, rangsNodes);

	}

	private static void setRangs(PartialTree pt, List<Node> rangsNodes) {
		for (Node node : rangsNodes) {
			Node tem = pt.findNodeByUid(node.getUid());
			if (tem != null) {
				tem.setStart(node.getStart());
				tem.setEnd(node.getEnd());
			}
		}
	}

	private List<Node> prepareRanges(PartialTree pt) throws IOException, SyncException, InterruptedException {

		Map<Long, Node> rangsMap = new HashMap<>();
		while (true) {
			Message msg=peer.getCurrentMessage();
			if (msg == null) {
				break;
			}
			Node node = msg.getNode();

			long nodeUid = node.getUid();
			int nodePid = node.getPid();

			Node tem = rangsMap.get(nodeUid);
			if (tem == null) {
				tem = NodeFactory.createNode(node.getTagName(), node.getType(), nodePid);
				tem.setUid(nodeUid);
				rangsMap.put(nodeUid, tem);
			}

			if (tem.getStart() > nodePid) {
				tem.setStart(nodePid);
			}

			if (tem.getEnd() < nodePid) {
				tem.setEnd(nodePid);
			}

		}

		return new ArrayList<>(rangsMap.values());
	}

	private void computeUid(Node root) throws IOException, SyncException, InterruptedException {

		Message msg=peer.getCurrentMessage();
		long uid = -1;
		if (msg != null) {
			Node uidRecordNode = msg.getNode();
			uid = uidRecordNode.getUid();
		}

		Node p = root;
		while (p != null) {
			msg=peer.getCurrentMessage();
			if (msg == null) {
				break;
			}
			Node rec = msg.getNode();
			p.setUid(rec.getUid());
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
			com.sendNode(pid + 1, tem);
			p = root;
			while (p != null && (p.isRightOpenNode() || p.isPreOpenNode())) {
				com.sendNode(pid + 1, p);
				p = p.getLastChild();
			}
		}

	}

	private Node addPrePath(List<Node> subTrees) throws IOException, SyncException, InterruptedException {

		List<Node> pp = com.receiveNodeList();

		for (int i = 0; i < pp.size(); i++) {
			Node p = pp.get(i);
			p.setType(NodeType.PRE_NODE);
		}

		for (int i = 0; i < pp.size() - 1; i++) {
			Node p1 = pp.get(i);
			Node p2 = pp.get(i + 1);
			p1.addLastChild(p2);
		}

		Node root = NodeFactory.createNode("Root", NodeType.PRE_NODE, pid);
		if (pp.size() == 0) {
			for (Node node : subTrees) {
				root.addLastChild(node);
			}
		} else {
			Node last = pp.get(pp.size() - 1);
			for (Node node : subTrees) {
				last.addLastChild(node);
			}
			root.addLastChild(pp.get(0));
		}

		return root;
	}

	private void computPrePath(List<Node> subTrees) throws IOException, SyncException, InterruptedException {

		// collecting left open nodes and right open nodes.

		List<List<Node>> lls = new ArrayList<>(taskNum);
		List<List<Node>> rls = new ArrayList<>(taskNum);

		for (int i = 0; i < taskNum; i++) {
			List<Node> ll = new ArrayList<>();
			List<Node> rl = new ArrayList<>();
			lls.add(ll);
			rls.add(rl);
		}

		while (true) {
			Message msg = peer.getCurrentMessage();
			if (msg == null) {
				break;
			}
			Node node = msg.getNode();
			if (node.isLeftOpenNode()) {
				lls.get(node.getPid()).add(node);
			}
			if (node.isRightOpenNode()) {
				rls.get(node.getPid()).add(node);
			}
		}

		List<Node> auxList = new ArrayList<>();

		for (int i = 0; i < taskNum - 1; i++) {
			List<Node> rl = rls.get(i);
			for (Node node : rl) {
				auxList.add(node);
			}
			List<Node> ll = lls.get(i + 1);
			for (int j = 0; j < ll.size(); j++) {
				auxList.remove(auxList.size() - 1);
			}
			for (Node node : auxList) {
//				Node tem = NodeFactory.createNode(node.getTagName(), node.getType(), node.getPid());
				com.sendNode(i + 1, node);
			}
		}

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

	private static List<Node> selectOpenNodes(PartialTree pt) {

		Node root = pt.getRoot();

		List<Node> openNodes = new ArrayList<>();
		Node p = root;
		while (p != null && p.isPreOpenNode()) {
			openNodes.add(p);
			p = p.getFirstChild();
		}

		if (p != null) {
			Node parent = p.getParent();
			List<Node> chs = null;
			if (parent != null) {
				chs = parent.getAllChilds();
			} else {
				chs = p.getAllChilds();
			}
			openNodes.addAll(selectLeftOpenNodes(chs));
			openNodes.addAll(selectRightOpenNodes(chs));
		}

		return openNodes;

	}

	public List<Node> buildSubTrees() throws IOException, SyncException, InterruptedException {

		Deque<Node> stack = new ArrayDeque<Node>();
		stack.push(NodeFactory.createNode("ROOT", NodeType.CLOSED_NODE, pid));

		LongWritable key = new LongWritable();
		Text value = new Text();

		while (peer.readNext(key, value)) {

			Tag tag = getTag(value.toString().trim());

			if (tag != null) {

				// System.out.println(tag.toString());

				if (TagType.START.equals(tag.getType())) {

					Node node = NodeFactory.createNode(tag.getName(), NodeType.CLOSED_NODE, pid);
					stack.push(node);

				} else {

					if (TagType.FULL.equals(tag.getType())) {
						Node node = NodeFactory.createNode(tag.getName(), NodeType.CLOSED_NODE, pid);
						stack.push(node);
					}

					Node node = stack.peek();

					if (node.getTagName().equals(tag.getName())) {
						stack.pop();
						stack.peek().addLastChild(node);
					} else {
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

}
