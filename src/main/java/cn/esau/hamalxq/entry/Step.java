package cn.esau.hamalxq.entry;

import java.util.List;

public class Step {

    private String nameTest;
    private Axis axis;
    private String predicateStr;
    
    private Step next;
    
    private Step predicate;
    
    private List<Step> pSteps;

    public Step() {
        // TODO Auto-generated constructor stub
        this.nameTest = "*";
        this.axis = Axis.DESCENDANT_OR_SELF;
    }

    public String getNameTest() {
        return nameTest;
    }

    public void setNameTest(String nameTest) {
        if (nameTest == null) {
            this.nameTest = "*";
            return;
        }
        this.nameTest = nameTest;
    }

    public Axis getAxis() {
        return axis;
    }

    public Step getNext() {
        return next;
    }

    public void setNext(Step next) {
        this.next = next;
    }

    public Step getPredicate() {
        return predicate;
    }

    public void setPredicate(Step predicate) {
        this.predicate = predicate;
    }

    public void setAxis(Axis axis) {
        if (axis == null) {
            this.axis = Axis.DESCENDANT_OR_SELF;
            return;
        }
        this.axis = axis;
    }

    public String getPredicateStr() {
        return predicateStr;
    }

    public void setPredicateStr(String predicate) {
        this.predicateStr = predicate;
    }

    public List<Step> getpSteps() {
        return pSteps;
    }

    public void setpSteps(List<Step> pSteps) {
        this.pSteps = pSteps;
    }

    @Override
    public String toString() {
        String s = axis + "::" + nameTest;

        if (predicate != null) {
            s += "[" + predicate.toXPath() + "]";
        }
        
        return s;
    }
    
    public String toXPath() {
        String s = axis + "::" + nameTest;

        if (predicate != null) {
            s += "[" + predicate.toXPath() + "]";
        }
        
        if(next!=null) {
            s+="/";
            s+=next.toXPath();
        }

        return s;
    }

}
