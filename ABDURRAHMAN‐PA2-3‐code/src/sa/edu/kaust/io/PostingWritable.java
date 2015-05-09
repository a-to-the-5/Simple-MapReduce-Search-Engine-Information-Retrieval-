package sa.edu.kaust.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PostingWritable implements Writable, Comparable<PostingWritable> {

	int docNo;
	int tf;
	
	public PostingWritable(){
	}
	
	public PostingWritable(int docNo, int tf) {
		super();
		this.docNo = docNo;
		this.tf = tf;
	}

	public int getDocNo() {
		return docNo;
	}

	public void setDocNo(int docNo) {
		this.docNo = docNo;
	}

	public int getTf() {
		return tf;
	}

	public void setTf(int tf) {
		this.tf = tf;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		docNo = in.readInt();
		tf = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(docNo);
		out.writeInt(tf);
	}

	@Override
	public String toString() {
		return "<"+docNo+","+tf+">";
	}

	@Override
	public int compareTo(PostingWritable o) {
		return o.tf-tf;
	}
	
	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
}
