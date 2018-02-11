package cn.esau.hamalxq.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPPeer;
import org.apache.hama.bsp.sync.SyncException;

import cn.esau.hamalxq.entry.Axis;
import cn.esau.hamalxq.entry.Link;
import cn.esau.hamalxq.entry.Message;
import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.NodeType;
import cn.esau.hamalxq.entry.PNode;
import cn.esau.hamalxq.entry.PartialTree;
import cn.esau.hamalxq.entry.RemoteNode;
import cn.esau.hamalxq.entry.Step;

public class PQuerier {

	private PartialTree pt;

	private BSPPeer<LongWritable, Text, Text, Text, Message> peer;

	private int taskNum = 0;

	private Communication com;

	private List<List<PNode>> resultLists;

	public PQuerier() throws IOException, SyncException, InterruptedException {
		super();
	}

	public PQuerier(PartialTree pt, BSPPeer<LongWritable, Text, Text, Text, Message> peer,
			Communication com) throws IOException, SyncException, InterruptedException {
		super();
		this.pt = pt;
		this.peer = peer;
		this.com = com;
		this.taskNum=peer.getNumPeers();
	}

	public void init(List<List<Node>> inputLists) {
		if(isMarster()) {
			resultLists = preparePredicate(inputLists);
		}
	}
	
	public void setInputLists(List<List<PNode>> resultLists) {
		this.resultLists = resultLists;
	}

	public List<List<Node>> getResultLists(){
		return null;
	}

	public void query(Step pstep) throws IOException, SyncException, InterruptedException {
		
		Step step=pstep;
		
		while(step!=null) {

			if (isMarster()) {
				com.sendPNodeLists(resultLists);
			}
			sync();
			
			List<PNode> inputList=com.receivePNodeList();
			queryWithAixs(step.getAxis(), inputList, step.getNameTest());
			
            Step predicate = step.getPredicate();
            if (predicate != null) {
                // Querying predicate. his block will be executed when a query has a predicate. 
            	
            	PQuerier pQuerier=new PQuerier(pt, peer, com);     
            	
            	if(isMarster()) {                	
                    List<List<PNode>> intermadiate = regroupResults(resultLists);             
                    pQuerier.setInputLists(intermadiate);
            	}
            	sync();
                
                pQuerier.query(predicate);

                List<List<Node>> nodeLists = null;
                if(isMarster()) {
                    nodeLists = new ArrayList<>();
                    for(int i=0;i<taskNum;i++) {
                    	nodeLists.add(new ArrayList<Node>());
                    }
                }
                sync();
                
                pQuerier.proccessPredicate(nodeLists);

                if(isMarster()) {
                    resultLists = filterResults(resultLists, nodeLists);
                }
                sync();
            	
            }

			step = step.getNext();
			sync();
		}

	}

    public static List<List<PNode>> regroupResults(List<List<PNode>> inputLists) {

        List<List<PNode>> outputLists = new ArrayList<List<PNode>>();

        for (int i = 0; i < inputLists.size(); i++) {
            List<PNode> list = inputLists.get(i);

            Set<Node> set = new HashSet<Node>();
            for (PNode pnode : list) {
                set.add(pnode.getNode());
            }

            List<PNode> plist = new ArrayList<>();
            for (Node node : set) {
                plist.add(new PNode(node, new Link(i, node.getUid())));
            }

            outputLists.add(plist);
        }

        return outputLists;

    }
    
    public List<List<PNode>> preparePredicate(List<List<Node>> inputLists) {

        List<List<PNode>> outputLists = new ArrayList<List<PNode>>();

        for (int i = 0; i < inputLists.size(); i++) {
            List<Node> list = inputLists.get(i);
            List<PNode> plist = new ArrayList<>();
            for (Node node : list) {
                plist.add(new PNode(node, new Link(i, node.getUid())));
            }
            outputLists.add(plist);
        }

        return outputLists;

    }


	public void queryWithAixs(Axis axis, List<PNode> inputList, String test)
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

	private void queryChid(List<PNode> inputList, String test) throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub

		List<PNode> result=pt.findChildPNodes(inputList, test);
		com.sendPNodeList(0, result);

		sync();

		if (isMarster()) {
			resultLists = com.receivePNodesFromAllPeer();
		}

		sync();

	}
	

    public void proccessPredicate(List<List<Node>> outputLists) throws IOException, SyncException, InterruptedException {
    	
    	if(isMarster()) {

            List<Link> allLinks = new ArrayList<>();
            for (List<PNode> list : resultLists) {
                for (PNode pNode : list) {
                    allLinks.add(pNode.getLink());
                }
            }

            Map<Integer, List<Node>> uidLists = new HashMap<>();

            for (Link link : allLinks) {
                List<Node> uids = uidLists.get(link.getPid());
                if (uids == null) {
                	uids=new ArrayList<Node>();
                    uidLists.put(link.getPid(), uids);
                }
                uids.add(new Node(link.getUid()));
            }
            
            for(Integer p: uidLists.keySet()) {
            	com.sendNodeList(p, uidLists.get(p));
            }
                        
    	}
    	
    	sync();
    	
    	List<Node> inputs=com.receiveNodeList();
    	List<Node> outputs=pt.findNodesByUid(inputs);
    	com.sendNodeList(0, outputs);
    	
    	sync();
    	
    	if(isMarster()) {
    		List<List<Node>> temOutputList=com.receiveNodesFromAllPeer();
    		for(int i=0;i<taskNum;i++) {
    			List<Node> tem = temOutputList.get(i);
    			List<Node> output=outputLists.get(i);
    			output.clear();
    			output.addAll(tem);
    		}
    	}
    	
    	sync();
    	
    	shareNodes(outputLists);

		sync();
    }
    

	public void shareNodes(List<List<Node>> outputLists) throws IOException, SyncException, InterruptedException {

		if (isMarster()) {
			List<Node> toBeShare = new ArrayList<Node>();
			Set<Long> toBeShareUidSet = new HashSet<>();
			for (int i = 0; i < taskNum; i++) {
				for (Node node : outputLists.get(i)) {
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
				List<Node> inputList = outputLists.get(i);

				set.addAll(inputList);
				set.addAll(responseLists.get(i));

				inputList.clear();
				inputList.addAll(set);

			}

		}

	}



	private void queryParent(List<PNode> inputList, String test)
			throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub

		com.sendPNodeList(0, pt.findParentPNodes(inputList, test));

		sync();

		if (isMarster()) {
			resultLists = com.receivePNodesFromAllPeer();
		}

		sync();

		sharePNodes();

	}

	public void sharePNodes() throws IOException, SyncException, InterruptedException {

		if (isMarster()) {
			List<PNode> toBeShare = new ArrayList<PNode>();
			Set<Long> toBeShareUidSet = new HashSet<>();
			for (int i = 0; i < taskNum; i++) {
				for (PNode pnode : resultLists.get(i)) {
					Node node=pnode.getNode();
					if (!NodeType.CLOSED_NODE.equals(node.getType()) && !toBeShareUidSet.contains(node.getUid())) {
						toBeShareUidSet.add(node.getUid());
						toBeShare.add(pnode);
					}
				}
			}

			com.sendPNodesToAllWorker(toBeShare);
		}

		sync();

		List<PNode> toBeShare = com.receivePNodeList();
		List<PNode> results = pt.findCorrespondingPNodes(toBeShare);
		com.sendPNodeList(0, results);

		sync();

		if (isMarster()) {

			List<List<PNode>> responseLists = com.receivePNodesFromAllPeer();

			for (int i = 0; i < taskNum; i++) {

				Set<PNode> set = new HashSet<PNode>();
				List<PNode> inputList = resultLists.get(i);

				set.addAll(inputList);
				set.addAll(responseLists.get(i));

				inputList.clear();
				inputList.addAll(set);

			}

		}

	}

	private void queryDescendant(List<PNode> inputList, String test)
			throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub

		com.sendPNodeList(0, pt.findDescendantPNodes(inputList, test));

		sync();

		if (isMarster()) {
			resultLists = com.receivePNodesFromAllPeer();
		}

		sync();

	}

	private void queryFollowingSibling(List<PNode> inputList, String test)
			throws IOException, SyncException, InterruptedException {
		// TODO Auto-generated method stub

		// Local query
		List<PNode> res1 = pt.findFolSibPNodes(inputList, test);
		com.sendPNodeList(0, res1);
		res1 = null;

		sync();

		List<List<PNode>> outputLists = null;

		if (isMarster()) {

			outputLists = com.receivePNodesFromAllPeer();

			List<List<PNode>> temList = new ArrayList<>();

			// Preparing remote query
			for (int i = 0; i < taskNum; i++) {

				List<PNode> tem = new ArrayList<>();
				List<PNode> input = resultLists.get(i);
				for (PNode pnode : input) {
					Node node=pnode.getNode();
					if (!node.isRightOpenNode() && !node.isPreOpenNode()) {
						tem.add(pnode);
					}
				}
				temList.add(tem);

			}

			com.sendPNodeLists(temList);

		}

		sync();

		List<PNode> inputs = com.receivePNodeList();

		List<PNode> res2 = pt.findParentPNodes(inputs);
		com.sendPNodeList(0, res2);
		res2 = null;

		sync();

		if (isMarster()) {

			List<List<PNode>> parentList = com.receivePNodesFromAllPeer();

			List<RemoteNode> toBeQueried = new ArrayList<RemoteNode>();
			for (int i = 0; i < taskNum; i++) {
				for (PNode pNode : parentList.get(i)) {
					Node parent=pNode.getNode();
					if (parent.isRightOpenNode() || parent.isPreOpenNode()) {
						toBeQueried.add(new RemoteNode(parent, i + 1, parent.getEnd(), pNode.getLink()));
					}
				}
			}

			// Regroup nodes by partial tree id
			List<List<PNode>> remoteInputLists = regroupPNodes(toBeQueried);

			com.sendPNodeLists(remoteInputLists);
		}

		sync();

		// Remote query
		List<PNode> remoteInputList = com.receivePNodeList();
		List<PNode> res3 = pt.findChildPNodes(remoteInputList, test);
		com.sendPNodeList(0, res3);
		res3 = null;

		sync();

		if (isMarster()) {
			List<List<PNode>> remoteOutputList = com.receivePNodesFromAllPeer();

			// Merge results of local query and remote query
			for (int i = 0; i < taskNum; i++) {

				List<PNode> tem = outputLists.get(i);
				List<PNode> remoteResult = remoteOutputList.get(i);

				Set<PNode> set = new HashSet<PNode>();
				set.addAll(tem);
				set.addAll(remoteResult);

				tem.clear();
				tem.addAll(set);
			}

			resultLists = outputLists;

		}

		sync();

	}

    private List<List<PNode>> regroupPNodes(List<RemoteNode> toBeQueried) {
        List<List<PNode>> remoteInputList = new ArrayList<>();

        for (int i = 0; i < taskNum; i++) {
            List<PNode> remoteInput = new ArrayList<>();
            Set<PNode> set = new HashSet<>();
            for (int j = 0; j < toBeQueried.size(); j++) {
                RemoteNode remoteNode = toBeQueried.get(j);
                if (remoteNode.st <= i && remoteNode.ed >= i) {
                    Node node = remoteNode.getNode();
                    set.add(new PNode(node, remoteNode.getLink()));
                }
            }
            remoteInput.addAll(set);
            remoteInputList.add(remoteInput);
        }
        return remoteInputList;
    }    
    
    public static List<List<PNode>> filterResults(List<List<PNode>> intermadiate, List<List<Node>> inputLists) {

        List<List<PNode>> outputLists = new ArrayList<List<PNode>>();

        int p = intermadiate.size();

        List<HashMap<Node, List<Link>>> pnodeMap = new ArrayList<HashMap<Node, List<Link>>>();
        for (int i = 0; i < p; i++) {

            HashMap<Node, List<Link>> map = new HashMap<Node, List<Link>>();
            pnodeMap.add(map);

            List<PNode> list = intermadiate.get(i);
            for (PNode pNode : list) {
                List<Link> links = map.get(pNode.getNode());

                if (links == null) {
                    links = new ArrayList<Link>();
                    map.put(pNode.getNode(), links);
                }

                links.add(pNode.getLink());
            }

        }

        for (int i = 0; i < p; i++) {
            HashMap<Node, List<Link>> map = pnodeMap.get(i);
            List<PNode> result = new ArrayList<>();
            List<Node> nodes = inputLists.get(i);

            for (Node node : nodes) {
                List<Link> links = map.get(node);
                if (links != null) {
                    for (Link link : links) {
                        result.add(new PNode(node, link));
                    }
                }
            }

            outputLists.add(result);
        }

        return outputLists;

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

	public PartialTree getPt() {
		return pt;
	}

	public void setPt(PartialTree pt) {
		this.pt = pt;
	}

}
