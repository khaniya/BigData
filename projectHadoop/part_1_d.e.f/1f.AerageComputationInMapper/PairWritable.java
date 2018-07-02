package inmapper;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class PairWritable implements Writable{
	private Float key;
	private Integer value;
	
	public PairWritable(Float key, Integer value){
		this.key = key;
		this.value = value;
	}
	
	public PairWritable() {
		
	}
	
	public Float getKey() {
		return key;
	}

	public void setKey(Float key) {
		this.key = key;
	}

	public Integer getValue() {
		return value;
	}

	public void setValue(Integer value) {
		this.value = value;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		key = in.readFloat();
		value = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeFloat(key);
		out.writeInt(value);
	}
}
