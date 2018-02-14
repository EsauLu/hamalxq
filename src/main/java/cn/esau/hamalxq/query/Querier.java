package cn.esau.hamalxq.query;

import java.io.IOException;
import java.util.ArrayList;
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
import cn.esau.hamalxq.entry.Message;
import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.NodeType;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.entry.RemoteNode;
import cn.esau.hamalxq.entry.Step;
import cn.esau.hamalxq.utils.Utils;

public class Querier {

	private PartialTree pt;

	private BSPPeer<LongWritable, Text, Text, Text, Message> peer;

	private Map<String, Step> xpathMap;

	private int taskNum = 0;

	private Communication com;

	private List<List<Node>> resultLists;

	private PQuerier pQuerier;

	public Querier() throws IOException, SyncException, InterruptedException {
		super();
	}

	public void start() throws IOException, SyncException, InterruptedException {

		com = new Communication(peer);
		pQuerier = new PQuerier(pt, peer, com);

		for (String key : xpathMap.keySet()) {
			Step xpath = xpathMap.get(key);

			resultLists = null;
			long t1 = 0, t2 = 0;

			if (isMarster()) {
				t1 = System.currentTimeMillis();
			}
			sync();

			query(xpath);

			if (isMarster()) {
				t2 = System.currentTimeMillis();
				printResultLists(t2 - t1, key, xpath);
			}
			sync();
		}

	}

	private void printResultLists(long timeOut, String key, Step xpath)
			throws IOException, SyncException, InterruptedException {
//		Utils.print(resultLists);
		peer.write(new Text(key+" : "), new Text("/"+xpath.toXPath()));
		peer.write(new Text(""), new Text(""));
		peer.write(new Text("Number of nodes in result : "), new Text(""));
		peer.write(new Text(""), new Text(""));
		long count = 0;
		for(int i=0;i<taskNum;i++) {
			List<Node> result=resultLists.get(i);
			for(Node node: result) {
			    if(node.isClosedNode()||node.isLeftOpenNode()) {
			        count++;
			    }
			}
			peer.write(new Text("\tpt"+i+" : "), new Text(""+result.size()));
	        peer.write(new Text(""), new Text(""));
		}
		peer.write(new Text(""), new Text(""));
		peer.write(new Text("Number of all nodes : "), new Text(""+count));
		peer.write(new Text(""), new Text(""));
		peer.write(new Text("Time out :"), new Text(""+timeOut+"ms"));
		peer.write(new Text(""), new Text(""));
		peer.write(new Text("-------"), new Text("---------------------------------------------------------"));
        peer.write(new Text(""), new Text(""));
	}

	private void query(Step xpath) throws IOException, SyncException, InterruptedException {

		com.sendNode(0, pt.getRoot());
		sync();

		if (isMarster()) {
			resultLists = com.receiveNodesFromAllPeer();
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

			Step predicate = step.getPredicate();
			if (predicate != null) {
				// Querying predicate. his block will be executed when a query has a predicate.

				pQuerier.init(resultLists);
				pQuerier.query(predicate);
				pQuerier.proccessPredicate(resultLists);

			}
			
//			if(isMarster()) {
//		        for(int i=0;i<taskNum;i++) {
//		            List<Node> result=resultLists.get(i);
//		            StringBuilder sb=new StringBuilder();
// 		            for(Node node: result) {
//		                sb.append(node);
//		            }
//		            peer.write(new Text("\tstep : "), new Text(step.toString()));
//                    peer.write(new Text("\tNode : "), new Text(sb.toString()));
//		            peer.write(new Text(""), new Text(""));
//		        }
//		        peer.write(new Text(""), new Text(""));
//			}
//			
//			sync();
            step = step.getNext();
            sync();
		}

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
			queryParent(inputList, test);
		}

		// Following-sibling axis
		if (Axis.FOLLOWING_SIBLING.equals(axis)) {
			queryFollowingSibling(inputList, test);
		}

		sync();

	}

	private void queryChid(List<Node> inputList, String test) throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub

		com.sendNodeList(0, pt.findChildNodes(inputList, test));

		sync();

		if (isMarster()) {
			resultLists = com.receiveNodesFromAllPeer();
		}

		sync();

	}

	private void queryParent(List<Node> inputList, String test)
			throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub

		com.sendNodeList(0, pt.findParentNodes(inputList, test));

		sync();

		if (isMarster()) {
			resultLists = com.receiveNodesFromAllPeer();
		}

		sync();

		shareNodes();

	}

	public void shareNodes() throws IOException, SyncException, InterruptedException {

		if (isMarster()) {
			List<Node> toBeShare = new ArrayList<Node>();
			Set<Long> toBeShareUidSet = new HashSet<>();
			for (int i = 0; i < taskNum; i++) {
				for (Node node : resultLists.get(i)) {
					if (!NodeType.CLOSED_NODE.equals(node.getType()) && !toBeShareUidSet.contains(node.getUid())) {
						toBeShareUidSet.add(node.getUid());
						toBeShare.add(node);
					}
				}
			}

			com.sendNodesToAllWorker(toBeShare);
		}

		sync();

		List<Node> toBeShare = com.receiveNodeList();
		List<Node> results = pt.findCorrespondingNodes(toBeShare);
		com.sendNodeList(0, results);

		sync();

		if (isMarster()) {

			List<List<Node>> responseLists = com.receiveNodesFromAllPeer();

			for (int i = 0; i < taskNum; i++) {

				Set<Node> set = new HashSet<Node>();
				List<Node> inputList = resultLists.get(i);

				set.addAll(inputList);
				set.addAll(responseLists.get(i));

				inputList.clear();
				inputList.addAll(set);

			}

		}

	}

	private void queryDescendant(List<Node> inputList, String test)
			throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub

		com.sendNodeList(0, pt.findDescendantNodes(inputList, test));

		sync();

		if (isMarster()) {
			resultLists = com.receiveNodesFromAllPeer();
		}

		sync();

	}

	private void queryFollowingSibling(List<Node> inputList, String test)
			throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub

		// Local query
		List<Node> res1 = pt.findFolSibNodes(inputList, test);
		com.sendNodeList(0, res1);
		res1 = null;

		sync();

		List<List<Node>> outputLists = null;

		if (isMarster()) {

			outputLists = com.receiveNodesFromAllPeer();

			List<List<Node>> temList = new ArrayList<>();

			// Preparing remote query
			for (int i = 0; i < taskNum; i++) {

				List<Node> tem = new ArrayList<>();
				List<Node> input = resultLists.get(i);
				for (Node node : input) {
					if (!node.isRightOpenNode() && !node.isPreOpenNode()) {
						tem.add(node);
					}
				}
				temList.add(tem);

			}

			com.sendNodeLists(temList);

		}

		sync();

		List<Node> inputs = com.receiveNodeList();

		List<Node> res2 = pt.findParentNodes(inputs);
		com.sendNodeList(0, res2);
		res2 = null;

		sync();

		if (isMarster()) {

			List<List<Node>> parentList = com.receiveNodesFromAllPeer();

			List<RemoteNode> toBeQueried = new ArrayList<RemoteNode>();
			for (int i = 0; i < taskNum; i++) {
				for (Node parent : parentList.get(i)) {
					if (parent.isRightOpenNode() || parent.isPreOpenNode()) {
						toBeQueried.add(new RemoteNode(parent, i + 1, parent.getEnd()));
					}
				}
			}

			// Regroup nodes by partial tree id
			List<List<Node>> remoteInputLists = regroupNodes(toBeQueried);

			com.sendNodeLists(remoteInputLists);
		}

		sync();

		// Remote query
		List<Node> remoteInputList = com.receiveNodeList();
		List<Node> res3 = pt.findChildNodes(remoteInputList, test);
		com.sendNodeList(0, res3);
		res3 = null;

		sync();

		if (isMarster()) {
			List<List<Node>> remoteOutputList = com.receiveNodesFromAllPeer();

			// Merge results of local query and remote query
			for (int i = 0; i < taskNum; i++) {

				List<Node> tem = outputLists.get(i);
				List<Node> remoteResult = remoteOutputList.get(i);

				Set<Node> set = new HashSet<Node>();
				set.addAll(tem);
				set.addAll(remoteResult);

				tem.clear();
				tem.addAll(set);
			}

			resultLists = outputLists;

		}

		sync();

	}

	private List<List<Node>> regroupNodes(List<RemoteNode> toBeQueried) {
		List<List<Node>> remoteInputList = new ArrayList<>();

		for (int i = 0; i < taskNum; i++) {
			List<Node> remoteInput = new ArrayList<>();
			Map<Long, Node> map = new HashMap<>();
			for (int j = 0; j < toBeQueried.size(); j++) {
				RemoteNode remoteNode = toBeQueried.get(j);
				if (remoteNode.st <= i && remoteNode.ed >= i) {
					Node node = remoteNode.getNode();
					if (!map.containsKey(node.getUid())) {
						map.put(node.getUid(), node);
					}
				}
			}
			remoteInput.addAll(map.values());
			remoteInputList.add(remoteInput);
		}
		return remoteInputList;
	}

	private void sync() throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub
		peer.sync();
	}

	private boolean isMarster() {
		return peer.getPeerIndex() == 0;
	}

	public BSPPeer<LongWritable, Text, Text, Text, Message> getPeer() {
		return peer;
	}

	public void setPeer(BSPPeer<LongWritable, Text, Text, Text, Message> peer) {
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
