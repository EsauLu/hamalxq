package cn.esau.hamalxq.parser;

import java.util.HashMap;
import java.util.Map;

import cn.esau.hamalxq.entry.Axis;
import cn.esau.hamalxq.entry.Step;

public class XPathParser {

    public static Map<String, Step> getXPaths(String[] xpaths) {

        Map<String, Step> xpathMap = new HashMap<>();

        for (String line : xpaths) {

            if (line.startsWith("#") || !line.contains(" ")) {
                continue;
            }

            int k = line.indexOf(' ');
            String key = line.substring(0, k);
            String xpath = line.substring(k + 1);
            xpathMap.put(key, parseXpath(xpath));

        }

        return xpathMap;
    }

    public static Step parseXpath(String xpath) {

        StringBuilder sb = new StringBuilder(xpath);
        if (sb.charAt(0) == '/') {
            sb.deleteCharAt(0);
        }

        int c = 0, st = 0;
        Step root = new Step();
        Step pre = root;
        for (int i = 0; i < sb.length(); i++) {

            if (c == 0 && sb.charAt(i) == '/') {
                String stepStr = sb.substring(st, i);
                st = i + 1;
                pre.setNext(parseStep(stepStr));
                pre = pre.getNext();
            }

            if (sb.charAt(i) == '[') {
                c++;
            }

            if (sb.charAt(i) == ']') {
                c--;
            }

        }
        String stepStr = sb.substring(st);
        pre.setNext(parseStep(stepStr));
        pre = pre.getNext();

        return root.getNext();
    }

    public static Step parseStep(String stepStr) {

        Step step = new Step();
        int i = stepStr.indexOf("::");
        step.setAxis(Axis.parseString(stepStr.substring(0, i)));

        int j = stepStr.indexOf("[");
        if (j == -1) {
            step.setNameTest(stepStr.substring(i + 2));
        } else {
            step.setNameTest(stepStr.substring(i + 2, j));
            Step predicate = parseXpath(stepStr.substring(j + 1, stepStr.length() - 1));
            step.setPredicate(predicate);
        }

        return step;
    }

}
