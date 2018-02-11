package cn.esau.hamalxq.entry;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class Message  implements WritableComparable<Message>{
	
	private Node node;
	private Link link;
	
	public Message() {
		super();
		// TODO Auto-generated constructor stub
	}
	public Message(Node node, Link link) {
		super();
		this.node = node;
		this.link = link;
	}
	@Override
	public int compareTo(Message o) {
		// TODO Auto-generated method stub
		return this.node.compareTo(o.node);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		
		boolean f=in.readBoolean();
		
		if(f) {
			node=new Node();
			node.readFields(in);
		}
		
		f=in.readBoolean();
		
		if(f) {
			link=new Link();
			link.readFields(in);
		}
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		if(node!=null) {
			out.writeBoolean(true);
			node.write(out);
		}else {
			out.writeBoolean(false);
		}
		
		if(link!=null) {
			out.writeBoolean(true);
			link.write(out);
		}else {
			out.writeBoolean(false);
		}
	}
	public Node getNode() {
		return node;
	}
	public void setNode(Node node) {
		this.node = node;
	}
	public Link getLink() {
		return link;
	}
	public void setLink(Link link) {
		this.link = link;
	}
	
}
