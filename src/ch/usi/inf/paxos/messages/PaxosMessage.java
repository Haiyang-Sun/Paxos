package ch.usi.inf.paxos.messages;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.messages.client.PaxosClientMessage;
import ch.usi.inf.paxos.roles.Client;

public abstract class PaxosMessage {
	GeneralNode from;
	public GeneralNode getFrom() {
		return from;
	}
	
	//global counter for all messages on this node
	protected int msgId;
	
	//used to identify different "rounds" of proposed values
	protected int slotIndex;
	
	public int getSlotIndex() {
		return slotIndex;
	}
	public PaxosMessage(GeneralNode from, int slotIndex){
		this.from = from;
		msgId = globalCount.incrementAndGet();
		this.slotIndex = slotIndex;
	}
	public PaxosMessage(GeneralNode from, int slotIndex, int id){
		this.from = from;
		this.msgId = id;
		this.slotIndex = slotIndex;
	}
	static AtomicInteger globalCount = new AtomicInteger();
	public ByteBuffer getMessageBytes(){
		return null;
	}
	public MessageType getType(){
		return MessageType.MSG_UNKONWN;
	}
	
	@Override
	public int hashCode(){
		ByteBuffer buf = getMessageBytes();
		return Arrays.toString(buf.array()).hashCode();
	}
	
	public int getPacketSize(){
		return getMessageBytes().position();
	}
	
	@Override
	public String toString(){
		ByteBuffer buf = getMessageBytes();
		
		int size = buf.position();
		buf.rewind();
		byte msgType = buf.get();
		int nodeId = buf.getInt();
		int slotIndex = buf.getInt();
		int msgId = buf.getInt();
		
		int position = buf.position();
		byte[] valueBuf = new byte[size - position];
		buf.get(valueBuf, 0, size - position);
		
		return "type:"+msgType+" node id:"+nodeId+" slot:"+slotIndex+" msgId "+msgId+" rest: "+Arrays.toString(valueBuf);
	}
	
	public static int VALUE_OFFSET = 1 + 4 + 4 + 4;
}
