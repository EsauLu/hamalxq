package cn.esau.hamalxq.entry;

public enum Axis {

    SELF, CHILD, PARENT, DESCENDANT, ANCESTOR, DESCENDANT_OR_SELF, ANCESTOR_OR_SELF, FOLLOWING_SIBLING, PRECEDING_SIBLING;

    public static Axis parseString(String axisName) {

        if (axisName == null) {
            return null;
        }

        if (axisName.equalsIgnoreCase("SELF")) {
            return SELF;
        }

        if (axisName.equalsIgnoreCase("CHILD")) {
            return CHILD;
        }

        if (axisName.equalsIgnoreCase("PARENT")) {
            return PARENT;
        }

        if (axisName.equalsIgnoreCase("DESCENDANT")) {
            return DESCENDANT;
        }

        if (axisName.equalsIgnoreCase("ANCESTOR")) {
            return ANCESTOR;
        }

        if (axisName.equalsIgnoreCase("DESCENDANT-OR-SELF")) {
            return DESCENDANT_OR_SELF;
        }

        if (axisName.equalsIgnoreCase("ANCESTOR-OR-SELF")) {
            return ANCESTOR_OR_SELF;
        }

        if (axisName.equalsIgnoreCase("FOLLOWING-SIBLING")) {
            return FOLLOWING_SIBLING;
        }

        if (axisName.equalsIgnoreCase("PRECEDING-SIBLING")) {
            return PRECEDING_SIBLING;
        }

        return null;
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return super.toString().toLowerCase().replace('_', '-');
    }

}


























