package cn.esau.hamalxq.query;

import cn.esau.hamalxq.entry.PartialTree;

public class Querier {
    
    private PartialTree pt;

    public Querier(PartialTree pt) {
        super();
        this.pt = pt;
    }

    public PartialTree getPt() {
        return pt;
    }

    public void setPt(PartialTree pt) {
        this.pt = pt;
    }

}
