/*
 * Sifarish: Recommendation Engine
 * Author: Pranab Ghosh
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.sifarish.feature;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TextIntPair implements WritableComparable<TextIntPair> {
	private Text first;
	private IntWritable second;
	
	public TextIntPair() {
		first = new Text();
		second =  new IntWritable();
	}
	
	public void set(String first, int second) {
		this.first.set(first);
		this.second.set(second);
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
	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
		
	}
	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
		
	}
	@Override
	public int compareTo(TextIntPair tiPair) {
		int cmp = first.compareTo(tiPair.getFirst());
		if (0 == cmp) {
			cmp = second.compareTo(tiPair.getSecond());
		}
		return cmp;
	}
	
	public int hashCode() {
		return first.hashCode() * 163 + second.hashCode();
	}
	
	public boolean equals(Object obj) {
		boolean isEqual =  false;
		if (obj instanceof TextIntPair) {
			TextIntPair tiPair = (TextIntPair)obj;
			isEqual = first.equals(tiPair.first) && second.equals(tiPair.second);
		}
		
		return isEqual;
	}
	
	public String toString() {
		return first.toString() + ":" + second.get();
	}

}
