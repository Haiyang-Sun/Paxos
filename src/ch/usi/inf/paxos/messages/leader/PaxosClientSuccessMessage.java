package ch.usi.inf.paxos.messages.leader;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.roles.Client;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.*;

public class PaxosClientSuccessMessage extends PaxosMessage {
	Client client;
	
	/*
	 * constructor for the sender part
	 */
	public PaxosClientSuccessMessage(Client client, int slotIndex) {
		super(client, slotIndex);
		this.client = client;
	}
	
	/*
	 * constructor for the receiver part
	 */
	public PaxosClientSuccessMessage(Client client, int slotIndex, int id) {
		super(client, slotIndex, id);
		this.client = client;
	}
	
	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_CLIENT));
		bytes.putInt(client.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_CLIENT;
	}
}
