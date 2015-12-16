package ch.usi.inf.paxos.messages.acceptor;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Acceptor;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosPhase2BMessage extends PaxosMessage {
	
	Acceptor acceptor;
	long v_rnd;
	ValueType v_val;
	private boolean escapePhase1;

	public long getV_rnd() {
		return v_rnd;
	}

	public ValueType getV_val() {
		return v_val;
	}
	
	public PaxosPhase2BMessage(Acceptor acceptor, int slotIndex, long v_rnd, ValueType v_val, boolean escapePhase1) {
		super(acceptor, slotIndex);
		this.acceptor = acceptor;
		this.v_rnd = v_rnd;
		this.v_val = v_val;
		this.escapePhase1 = escapePhase1;
	}
	
	public PaxosPhase2BMessage(Acceptor acceptor, int slotIndex, long v_rnd, ValueType v_val, int msgId, boolean escapePhase1) {
		super(acceptor, slotIndex, msgId);
		this.acceptor = acceptor;
		this.v_rnd = v_rnd;
		this.v_val = v_val;
		this.escapePhase1 = escapePhase1;
	}

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_ACCEPTOR_PHASE2B));
		bytes.putInt(acceptor.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.putLong(v_rnd);
		bytes.putInt(escapePhase1?1:0);
		bytes.put(v_val.getValue());
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_ACCEPTOR_PHASE2B;
	}
	
	public boolean getEscapePhase1(){
		return this.escapePhase1;
	}

}
