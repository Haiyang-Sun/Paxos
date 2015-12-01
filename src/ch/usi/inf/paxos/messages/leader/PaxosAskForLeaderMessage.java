package ch.usi.inf.paxos.messages.leader;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Acceptor;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosAskForLeaderMessage extends PaxosMessage {
	private Acceptor acceptor;
	public PaxosAskForLeaderMessage(Acceptor node) {
		/*
		 * TODO, separte leader messages from paxos messages
		 */
		super(node, -1);
		this.acceptor = node;
	}
	
	public PaxosAskForLeaderMessage(Acceptor node, int msgId) {
		super(node, -1, msgId);
		this.acceptor = node;
	}
	

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_ACCEPTOR_ASK_FOR_LEADER));
		bytes.putInt(acceptor.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_ACCEPTOR_ASK_FOR_LEADER;
	}
}
