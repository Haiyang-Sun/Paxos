package ch.usi.inf.paxos.messages.proposer;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosPhase1AMessage extends PaxosMessage {
	
	private Proposer proposer;
	private long c_rnd;
	
	public long getC_rnd() {
		return c_rnd;
	}

	public PaxosPhase1AMessage(Proposer proposer, int slotIndex, long c_rnd) {
		super(proposer, slotIndex);
		this.proposer = proposer;
		this.c_rnd = c_rnd;
	}
	
	public PaxosPhase1AMessage(Proposer proposer, int slotIndex, long c_rnd, int msgId) {
		super(proposer, slotIndex, msgId);
		this.proposer = proposer;
		this.c_rnd = c_rnd;
	}

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_PROPOSER_PHASE1A));
		bytes.putInt(proposer.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.putLong(c_rnd);
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_PROPOSER_PHASE1A;
	}

}
