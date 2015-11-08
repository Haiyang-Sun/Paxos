package ch.usi.inf.paxos;

import java.util.Arrays;

public class ValueType {
	
	public static ValueType NIL = new ValueType("");
	
	byte[] value;

	public ValueType(String value) {
		super();
		this.value = value.getBytes();
	}
	
	public ValueType(byte[] value, int start, int length) {
		this.value = Arrays.copyOfRange(value, start, start+length);
	}
	public ValueType(byte[] value, int start) {
		this.value = Arrays.copyOfRange(value, start, value.length);
	}
	public ValueType(byte[] value) {
		this.value = Arrays.copyOfRange(value, 0, value.length);
	}
	
	public byte[] getValue() {
		return value;
	}

//	public String getStringValue(){
//		return Arrays.toString(value);
//	}
	@Override
	public String toString(){
		if(value !=null)
			return Arrays.toString(value);
		else
			return "";
	}
	
	@Override 
	public int hashCode(){
		return this.toString().hashCode();
	}
	
	@Override 
	public boolean equals(Object other){
		if(other == null)
			return false;
		if(!(other instanceof ValueType))
			return false;
		ValueType otherValue = (ValueType) other;
		return otherValue.toString().equals(this.toString());
	}
}
