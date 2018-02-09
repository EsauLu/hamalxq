package cn.esau.hamalxq.query;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.Step;


public class QueryExecutor {

    private int p = 0;

    private List<Integer> pidList;
//
//    private ClientManager clientManager;
//
//    public QueryExecutor(List<Integer> pidList, ClientManager clientManager) {
//        super();
//        this.pidList = pidList;
//        this.clientManager = clientManager;
//        this.p = pidList.size();
//    }
//
    public List<List<Node>> query(Step steps) {
//
//        List<List<Node>> resultList = new ArrayList<List<Node>>();
//
//        LxqRequest request = new LxqRequestImpl();
//        request.setCode(LxqRequest.GET_ROOT);
//
//        for (int i = 0; i < p; i++) {
//            List<Node> tem = new ArrayList<>();
//            int pid = pidList.get(i);
//            clientManager.sendRequest(pid, request);
//            LxqResponse response = clientManager.getResponse(pid);
//            List<MsgItem> result = response.getResultList();
//            tem.add((Node)result.get(0));
//            resultList.add(tem);
//        }
//
//        Step step = steps;
//        while (step != null) {
//
//            resultList = queryWithAixs(step.getAxis(), resultList, step.getNameTest());
//
//            Step predicate = step.getPredicate();
//            if (predicate != null) {
//                // Querying predicate. his block will be executed when a query has a predicate.
//
//                PQueryExecutor pQueryExecutor = new PQueryExecutor(pidList, clientManager);
//
//                List<List<PNode>> intermadiate = pQueryExecutor.preparePredicate(resultList);
//
//                intermadiate = pQueryExecutor.predicateQuery(predicate, intermadiate);
//
//                resultList = pQueryExecutor.proccessPredicate(intermadiate);
//
//            }
//
//            step = step.getNext();
//
//        }
//
//        return resultList;
        return null;
    }
//
//    public List<List<Node>> queryWithAixs(Axis axis, List<List<Node>> inputLists, String test) {
//
//        // Child axis
//        if (Axis.CHILD.equals(axis)) {
//            return queryChid(inputLists, test);
//        }
//
//        // Descendant axis
//        if (Axis.DESCENDANT.equals(axis)) {
//            return queryDescendant(inputLists, test);
//        }
//
//        // Parent axis
//        if (Axis.PARENT.equals(axis)) {
//            return queryParent(inputLists, test);
//        }
//
//        // Following-sibling axis
//        if (Axis.FOLLOWING_SIBLING.equals(axis)) {
//            return queryFollowingSibling(inputLists, test);
//        }
//
//        return null;
//    }
//
//    public List<List<Node>> queryChid(List<List<Node>> inputLists, String test) {
//
//        LxqRequest request = new LxqRequestImpl();
//        request.setCode(LxqRequest.FIND_CHILD_NODES);
//        request.setMsg(test);
//
//        return sendFindRequests(request, inputLists);
//
//    }
//
//    public List<List<Node>> queryDescendant(List<List<Node>> inputLists, String test) {
//
//        LxqRequest request = new LxqRequestImpl();
//        request.setCode(LxqRequest.FIND_DESCENDANT_NODES);
//        request.setMsg(test);
//
//        return sendFindRequests(request, inputLists);
//
//    }
//
//    public List<List<Node>> queryParentIgnoreCNode(List<List<Node>> inputLists, String test) {
//        LxqRequest request = new LxqRequestImpl();
//        request.setCode(LxqRequest.FIND_PARENT_NODES);
//        request.setMsg(test);
//        return sendFindRequests(request, inputLists);
//
//    }
//
//    public List<List<Node>> queryParent(List<List<Node>> inputLists, String test) {
//        return shareNodes(queryParentIgnoreCNode(inputLists, test));
//
//    }
//
//    private List<List<Node>> sendFindRequests(LxqRequest request, List<List<Node>> inputLists) {
//
//        for (int i = 0; i < p; i++) {
//            int pid = pidList.get(i);
//            List<Node> input = inputLists.get(i);
//            request.setInputList(ListUtils.convertNodeList(input));
//            clientManager.sendRequest(pid, request);
//        }
//
//        List<LxqResponse> resposeLists = clientManager.getResponseList(pidList);
//
//        return ListUtils.recoverNodeListByResponse(resposeLists);
//    }
//
//    public List<List<Node>> shareNodes(List<List<Node>> nodeLists) {
//
//        List<Node> toBeShare = new ArrayList<Node>();
//        Set<Long> toBeShareUidSet = new HashSet<>();
//        for (int i = 0; i < p; i++) {
//            for (Node node : nodeLists.get(i)) {
//                if (!NodeType.CLOSED_NODE.equals(node.getType())                        
//                        && !toBeShareUidSet.contains(node.getUid())) {
//                    toBeShareUidSet.add(node.getUid());
//                    toBeShare.add(node);
//                }
//            }
//        }
//
//        LxqRequest request = new LxqRequestImpl();
//        request.setCode(LxqRequest.SHARE_NODES);
//        request.setInputList(ListUtils.convertNodeList(toBeShare));
//        clientManager.sendRequests(request);
//
//        List<List<Node>> responseLists = ListUtils.recoverNodeListByResponse(clientManager.getResponseList(pidList));
//
//        for (int i = 0; i < p; i++) {
//
//            Set<Node> set = new HashSet<Node>();
//            List<Node> inputList = nodeLists.get(i);
//
//            set.addAll(inputList);
//            set.addAll(responseLists.get(i));
//
//            inputList.clear();
//            inputList.addAll(set);
//
//        }
//
//        return nodeLists;
//    }
//
//    public List<List<Node>> queryFollowingSibling(List<List<Node>> inputLists, String test) {
//
//        // Local query
//        LxqRequest request = new LxqRequestImpl();
//        request.setCode(LxqRequest.FIND_FOLSIB_NODES);
//        request.setMsg(test);
//        List<List<Node>> outputList = sendFindRequests(request, inputLists);
//
//        // Preparing remote query
//        List<RemoteNode> toBeQueried = prepareRemoteQuery(inputLists);
//
//        // Regroup nodes by partial tree id
//        List<List<Node>> remoteInputList = regroupNodes(toBeQueried);
//
//        // Remote query
//        List<List<Node>> remoteOutputList = queryChid(remoteInputList, test);
//
//        // Merge results of local query and remote query
//        for (int i = 0; i < p; i++) {
//            List<Node> result = outputList.get(i);
//            List<Node> remoteResult = remoteOutputList.get(i);
//
//            Set<Node> set = new HashSet<Node>();
//            set.addAll(result);
//            set.addAll(remoteResult);
//
//            result.clear();
//            result.addAll(set);
//        }
//
//        return outputList;
//
//    }
//
//    private List<RemoteNode> prepareRemoteQuery(List<List<Node>> inputLists) {
//        // TODO Auto-generated method stub
//
//        List<List<Node>> parentList = new ArrayList<>();
//        List<RemoteNode> toBeQueried = new ArrayList<RemoteNode>();
//        for (int i = 0; i < p; i++) {
//
//            List<Node> tem = new ArrayList<>();
//            List<Node> input = inputLists.get(i);
//            for (Node node : input) {
//                if (!node.isRightOpenNode() && !node.isPreOpenNode()) {
//                    tem.add(node);
//                }
//            }
//            parentList.add(tem);
//
//        }
//
//        parentList = queryParentIgnoreCNode(parentList, "*");
//        for (int i = 0; i < p; i++) {
//            for (Node parent : parentList.get(i)) {
//                if (parent.isRightOpenNode() || parent.isPreOpenNode()) {
//                    toBeQueried.add(new RemoteNode(parent, i + 1, parent.getEnd()));
//                }
//            }
//        }
//        return toBeQueried;
//    }
//
//    private List<List<Node>> regroupNodes(List<RemoteNode> toBeQueried) {
//        List<List<Node>> remoteInputList = new ArrayList<>();
//
//        for (int i = 0; i < p; i++) {
//            List<Node> remoteInput = new ArrayList<>();
//            Map<Long, Node> map = new HashMap<>();
//            for (int j = 0; j < toBeQueried.size(); j++) {
//                RemoteNode remoteNode = toBeQueried.get(j);
//                if (remoteNode.st <= i && remoteNode.ed >= i) {
//                    Node node = remoteNode.getNode();
//                    if (!map.containsKey(node.getUid())) {
//                        map.put(node.getUid(), node);
//                    }
//                }
//            }
//            remoteInput.addAll(map.values());
//            remoteInputList.add(remoteInput);
//        }
//        return remoteInputList;
//    }

}
