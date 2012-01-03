package org.sifarish.feature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextIntInt implements WritableComparable<TextIntInt>{
	private Text first;
	private IntWritable second;
	private IntWritable third;

	public TextIntInt() {
		first = new Text();
		second =  new IntWritable();
		third =  new IntWritable();
	}
	
	public void set(String first, int second, int third) {
		this.first.set(first);
		this.second.set(second);
		this.third.set(third);
	}
	
	public Text getFirst() {
		return first;
	}
	public void setFirst(Text first) {
		this.first = first;
	}
	public IntWritable getSecond() {
		return second;
	}
	public void setSecond(IntWritable second) {
		this.second = second;
	}
	
	public IntWritable getThird() {
		return third;
	}

	public void setThird(IntWritable third) {
		this.third = third;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		third.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		third.write(out);
	}

	@Override
	public int compareTo(TextIntInt other) {
		int cmp = first.compareTo(other.getFirst());
		if (0 == cmp) {
			cmp = second.compareTo(other.getSecond());
		}
		if (0 == cmp) {
			cmp = third.compareTo(other.getThird());
		}
		return cmp;
	}


	public int compareToBase(TextIntInt other) {
		int cmp = first.compareTo(other.getFirst());
		if (0 == cmp) {
			cmp = second.compareTo(other.getSecond());
		}
		return cmp;
	}

	public int hashCode() {
		return first.hashCode() * 83 +  second.hashCode() * 17 + third.hashCode();
	}
	
	public int hashCodeBase() {
		return first.hashCode() * 83 +  second.hashCode();
	}

	public boolean equals(Object obj) {
		boolean isEqual =  false;
		if (obj instanceof TextIntInt) {
			TextIntInt other = (TextIntInt)obj;
			isEqual = first.equals(other.first) && second.equals(other.second) && third.equals(other.third);
		}
		
		return isEqual;
	}

}
