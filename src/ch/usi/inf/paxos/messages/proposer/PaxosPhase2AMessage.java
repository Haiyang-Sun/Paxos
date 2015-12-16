package ch.usi.inf.paxos.messages.proposer;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosPhase2AMessage extends PaxosMessage {
	
	private Proposer proposer;
	private long c_rnd;
	private ValueType c_val;
	private boolean escapePhase1;
	
	public ValueType getC_val() {
		return c_val;
	}

	public long getC_rnd() {
		return c_rnd;
	}

	public PaxosPhase2AMessage(Proposer proposer, int slotIndex, long c_rnd, ValueType c_val, boolean escapePhase1) {
		super(proposer, slotIndex);
		this.proposer = proposer;
		this.c_rnd = c_rnd;
		this.c_val = c_val;
		this.escapePhase1 = escapePhase1;
	}
	
	public PaxosPhase2AMessage(Proposer proposer, int slotIndex, long c_rnd, ValueType c_val, int msgId, boolean escapePhase1) {
		super(proposer, slotIndex, msgId);
		this.proposer = proposer;
		this.c_rnd = c_rnd;
		this.c_val = c_val;
		this.escapePhase1 = escapePhase1;
	}

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_PROPOSER_PHASE2A));
		bytes.putInt(proposer.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.putLong(c_rnd);
		bytes.putInt(escapePhase1?1:0);
		bytes.put(c_val.getValue());
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_PROPOSER_PHASE2A;
	}
	
	public boolean getEscapePhase1(){
		return this.escapePhase1;
	}

}
