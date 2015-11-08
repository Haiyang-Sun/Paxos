package ch.usi.inf.paxos.messages.acceptor;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Acceptor;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosPhase1BMessage extends PaxosMessage {
	
	Acceptor acceptor;
	long rnd;
	long v_rnd;
	ValueType v_val;
	public long getRnd() {
		return rnd;
	}

	public long getV_rnd() {
		return v_rnd;
	}

	public ValueType getV_val() {
		return v_val;
	}

	public PaxosPhase1BMessage(Acceptor acceptor, int slotIndex, long rnd, long v_rnd, ValueType v_val) {
		super(acceptor, slotIndex);
		this.acceptor = acceptor;
		this.rnd = rnd;
		this.v_rnd = v_rnd;
		this.v_val = v_val;
	}
	
	public PaxosPhase1BMessage(Acceptor acceptor, int slotIndex, long rnd, long v_rnd, ValueType v_val, int msgId) {
		super(acceptor, slotIndex, msgId);
		this.acceptor = acceptor;
		this.rnd = rnd;
		this.v_rnd = v_rnd;
		this.v_val = v_val;
	}

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_ACCEPTOR_PHASE1B));
		bytes.putInt(acceptor.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.putLong(rnd);
		bytes.putLong(v_rnd);
		bytes.put(v_val.getValue());
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_ACCEPTOR_PHASE1B;
	}

}
