package ch.usi.inf.paxos.messages.leader;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Client;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosClientSuccessMessage extends PaxosMessage {
	Proposer proposer;
	int clientId;
	
	/*
	 * constructor for the sender part
	 */
	public PaxosClientSuccessMessage(Proposer proposer, int clientId, int slotIndex) {
		super(proposer, slotIndex);
		this.proposer = proposer;
		this.clientId = clientId;
	}
	
//	/*
//	 * constructor for the receiver part
//	 */
//	public PaxosClientSuccessMessage(Proposer proposer, int slotIndex,  int id) {
//		super(proposer, slotIndex, id);
//		this.proposer = proposer;
//	}
	
	public int getClientId(){
		return this.clientId;
	}
	
	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_PROPOSER_CLIENT_SUCCESS));
		bytes.putInt(proposer.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.putInt(clientId);
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_PROPOSER_CLIENT_SUCCESS;
	}
}
