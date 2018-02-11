package cn.esau.hamalxq.utils;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

import cn.esau.hamalxq.entry.Node;
import cn.esau.hamalxq.entry.NodeType;
import cn.esau.hamalxq.entry.PNode;
import cn.esau.hamalxq.entry.RemoteNode;

public class Utils {

    public static void bfs(int pid, Node root) {

        if (root == null) {
            return;
        }

        // List<Node> list = root.getChildList();
        StringBuilder sb=new StringBuilder();
        sb.append(pid+" : ");

        for (int i = 0; i < root.getChildNum(); i++) {

            Node ch = root.getChildByIndex(i);

            Deque<Node> que = new ArrayDeque<>();
            que.addLast(ch);

            while (!que.isEmpty()) {

                Node node = que.removeFirst();

                if (NodeType.CLOSED_NODE.equals(node.getType())) {
                	sb.append(node);
                } else {
                    String s = node.toString();
                    s = s.substring(0, s.length() - 1);
                    sb.append(s+" ");
                }

                for (int j = 0; j < node.getChildNum(); j++) {
                    que.addLast(node.getChildByIndex(j));
                }

            }

        }

        System.out.println(sb.toString());

    }

    public static void bfsWithRoot(int pid, Node root) {

        if (root == null) {
            return;
        }
        
        StringBuilder sb=new StringBuilder();
        sb.append(pid+" : ");

        List<Node> list = new ArrayList<>();
        list.add(root);

        for (Node ch : list) {

            Deque<Node> que = new ArrayDeque<>();
            que.addLast(ch);

            while (!que.isEmpty()) {

                Node node = que.removeFirst();

                if (NodeType.CLOSED_NODE.equals(node.getType())) {
//                    System.out.print(node);
                	sb.append(node);
                } else {
                    String s = node.toString();
                    s = s.substring(0, s.length() - 1);
//                    System.out.print(s + " ");
                    sb.append(s+" ");
                }

                for (int j = 0; j < node.getChildNum(); j++) {
                    que.addLast(node.getChildByIndex(j));
                }

            }

        }

        System.out.println(sb.toString());

    }

    public static void bfsWithRanges(int pid, Node root) {

        StringBuilder sb = new StringBuilder();

        sb.append("pid = " + pid + "  >>  ");

        if (root == null) {
            return;
        }

        Deque<Node> que = new ArrayDeque<>();
        que.addLast(root);

        while (!que.isEmpty()) {

            Node node = que.removeFirst();

            if (NodeType.CLOSED_NODE.equals(node.getType())) {
                sb.append(node);
            } else {
                String s = node.toString();
                s = s.substring(0, s.length() - 1);
                sb.append(s + "(" + node.getStart() + ", " + node.getEnd() + ") ");
            }

            for (int j = 0; j < node.getChildNum(); j++) {
                que.addLast(node.getChildByIndex(j));
            }

        }

        System.out.println(sb.toString());

    }

    public static void bfsWithDepth(int pid, Node root) {

        if (root == null) {
            return;
        }

        StringBuilder sb = new StringBuilder();

        sb.append("pid = " + pid + "    ");

        Deque<Node> que = new ArrayDeque<>();
        que.addLast(root);

        while (!que.isEmpty()) {

            Node node = que.removeFirst();

            String s = node.toString();
            s = s.substring(0, s.length() - 1);
            sb.append(s + "(" + node.getDepth() + ") ");

            for (int j = 0; j < node.getChildNum(); j++) {
                que.addLast(node.getChildByIndex(j));
            }

        }

        System.out.println(sb.toString());

    }

    public static void dfsWithDepth(int pid, Node root) {
        StringBuilder sb=new StringBuilder();
        sb.append(pid+" : ");

        for (int i = 0; i < root.getChildNum(); i++) {

            Node ch = root.getChildByIndex(i);

            Deque<Node> stack = new ArrayDeque<Node>();
            stack.push(ch);

            while (!stack.isEmpty()) {
                Node node = stack.pop();

                String s = node.toString();
                s = s.substring(0, s.length() - 1);
//                System.out.print(s + "(" + node.getDepth() + ") ");
                sb.append(s + "(" + node.getDepth() + ") ");

                for (int j = node.getChildNum() - 1; j >= 0; j--) {
                    stack.push(node.getChildByIndex(j));
                }

            }
        }

        System.out.println(sb.toString());

    }

    public static void dfs(int pid, Node root) {

        StringBuilder sb = new StringBuilder();

        sb.append("pid :" + pid + " >>  ");

        Deque<Node> stack = new ArrayDeque<Node>();
        stack.push(root);

        while (!stack.isEmpty()) {
            Node node = stack.pop();

            String s = node.toString();
            s = s.substring(0, s.length() - 1);

            sb.append(s + "(" + node.getDepth() + ") ");

            for (int j = node.getChildNum() - 1; j >= 0; j--) {
                stack.push(node.getChildByIndex(j));
            }

        }

        System.out.println(sb.toString());

    }

    public static void print(List<List<Node>> results) {

        if (results == null) {
            System.out.println("null list");
            return;
        }

        StringBuilder sb = new StringBuilder();

        sb.append("\n");
        
        int p = results.size();
        for (int j = 0; j < p; j++) {
            List<Node> result = results.get(j);
            sb.append("  pt" + j + " : ");
            if (result == null) {
                sb.append("null pt");
                continue;
            }
            for (Node node : result) {
                sb.append(node);
            }

            sb.append("\n");
        }
        sb.append("\n");
        sb.append("---------------------------------------------");
        System.out.println(sb.toString());

    }

    public static void printPNodeList(List<List<PNode>> results) {

        if (results == null) {
            System.out.println("null list");
            return;
        }

        StringBuilder sb = new StringBuilder();

        sb.append("\n");
        
        int p = results.size();
        for (int j = 0; j < p; j++) {
            List<PNode> result = results.get(j);
            sb.append("  pt" + j + " : ");
            if (result == null) {
                sb.append("null pt");
                continue;
            }
            for (PNode node : result) { 
                sb.append(node);
            }

            sb.append("\n");
        }
        sb.append("\n");
        sb.append("----------------------------------------------------------");

        System.out.println(sb.toString());
    }

    public static void printNods(int pid, List<Node> list) {
        StringBuilder sb=new StringBuilder();
        
        sb.append(pid+" : ");
        for (Node node : list) {
            sb.append(node);
        }
        System.out.println(sb.toString());
    }

    public static void printPNods(int pid,List<PNode> list) {
    	
        StringBuilder sb=new StringBuilder();
        
        sb.append(pid+" : ");
        for (PNode node : list) {
            sb.append(node);
        }
        System.out.println(sb.toString());
    }

    public static void printRemoteNods(int pid,List<RemoteNode> list) {
    	
        StringBuilder sb=new StringBuilder();
        
        sb.append(pid+" : ");
        for (RemoteNode node : list) {
            sb.append(node);
        }
        System.out.println(sb.toString());
    }

}
