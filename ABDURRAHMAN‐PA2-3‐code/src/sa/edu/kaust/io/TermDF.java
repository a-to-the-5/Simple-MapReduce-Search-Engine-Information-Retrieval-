package sa.edu.kaust.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.WritableComparable;


public class TermDF implements WritableComparable<TermDF> {
	String[] k_gram;
	int df;
	
	public String[] getK_gram() {
		return k_gram;
	}

	public void setK_gram(String[] k_gram) {
		this.k_gram = k_gram;
	}

	public int getDf() {
		return df;
	}

	public void setDf(int df) {
		this.df = df;
	}

	public TermDF() {
	}
	
	public TermDF(String[] k, int d) {
		k_gram = k;
		df = d;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		int num = in.readInt();
		k_gram = new String[num];
		for (int i = 0; i < k_gram.length; i++) {
			k_gram[i] = in.readUTF();
		}
		df = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(k_gram.length);
		for (int i = 0; i < k_gram.length; i++) {
			out.writeUTF(k_gram[i]);
		}
		out.writeInt(df);
	}

	@Override
	public String toString() {
		return "<"+Arrays.toString(k_gram)+","+df+">";
	}

	@Override
	public int compareTo(TermDF o) {
		for (int i = 0; i < k_gram.length && i<o.k_gram.length; i++) {
			int r = k_gram[i].compareTo(o.k_gram[i]);
			if(r!=0) return r;
		}
		return k_gram.length-o.k_gram.length;
	}
	
	@Override
	public boolean equals(Object obj) {
		if(!(obj instanceof TermDF)) return false;
		return Arrays.equals(k_gram, ((TermDF)obj).k_gram);
	}
	
	@Override
	public int hashCode() {
		return Arrays.hashCode(k_gram);
	}
}
