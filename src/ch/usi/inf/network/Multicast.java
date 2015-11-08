package ch.usi.inf.network;

import java.nio.ByteBuffer;

public abstract class Multicast {
	public void send(byte[] buf, int start, int length){
	}
	public void send(ByteBuffer bytes) {
		send(bytes.array(), 0, bytes.position());
	}
	public ByteBuffer receive(int maxLength){
		return null;
	}
	
}
