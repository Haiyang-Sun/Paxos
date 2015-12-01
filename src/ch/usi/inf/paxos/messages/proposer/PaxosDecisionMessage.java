package ch.usi.inf.paxos.messages.proposer;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosDecisionMessage extends PaxosMessage {
	
	private Proposer proposer;
	private ValueType decision;
	
	public ValueType getDecision() {
		return decision;
	}

	public PaxosDecisionMessage(Proposer proposer, int slotIndex, ValueType decision) {
		super(proposer, slotIndex);
		this.proposer = proposer;
		this.decision = decision;
	}
	
	public PaxosDecisionMessage(Proposer proposer, int slotIndex, ValueType decision, int msgId) {
		super(proposer, slotIndex, msgId);
		this.proposer = proposer;
		this.decision = decision;
	}
	

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_PROPOSER_DECIDE));
		bytes.putInt(proposer.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.put(decision.getValue());
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_PROPOSER_DECIDE;
	}

}
