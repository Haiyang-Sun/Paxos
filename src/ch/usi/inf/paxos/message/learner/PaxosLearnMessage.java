package ch.usi.inf.paxos.message.learner;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Learner;

public class PaxosLearnMessage extends PaxosMessage {
	Learner learner;
	int requireSlot;
	
	/*
	 * constructor for the sender part
	 */
	public PaxosLearnMessage (Learner learner, int requireSlot, int slotIndex) {
		super(learner, slotIndex);
		this.learner = learner;
		this.requireSlot = requireSlot;
	}
	
//	/*
//	 * constructor for the receiver part
//	 */
//	public PaxosClientSuccessMessage(Proposer proposer, int slotIndex,  int id) {
//		super(proposer, slotIndex, id);
//		this.proposer = proposer;
//	}
	
	public int getRequireSlot(){
		return this.requireSlot;
	}
	
	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_LEARNER_LEARN));
		bytes.putInt(learner.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.putInt(requireSlot);
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_LEARNER_LEARN;
	}
}
