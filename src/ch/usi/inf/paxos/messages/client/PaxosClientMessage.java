package ch.usi.inf.paxos.messages.client;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.roles.Client;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.*;

public class PaxosClientMessage extends PaxosMessage {
	Client client;
	ValueType value;
	
	/*
	 * constructor for the sender part
	 */
	public PaxosClientMessage(Client client, ValueType value, int slotIndex) {
		super(client, slotIndex);
		this.client = client;
		this.value = value;
	}
	
	/*
	 * constructor for the receiver part
	 */
	public PaxosClientMessage(Client client, ValueType value,int slotIndex, int id) {
		super(client, slotIndex, id);
		this.client = client;
		this.value = value;
	}
	
	public ValueType getValue() {
		return value;
	}

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_CLIENT));
		bytes.putInt(client.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.put(value.getValue());
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_CLIENT;
	}
}
