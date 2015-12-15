package ch.usi.inf.paxos.messages.leader;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Acceptor;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosLeaderHeartBeatMessage extends PaxosMessage {
	private Proposer sender;
	public PaxosLeaderHeartBeatMessage(Proposer node) {
		/*
		 * TODO, separte leader messages from paxos messages
		 */
		super(node, -1);
		sender = node;
	}
	
	public PaxosLeaderHeartBeatMessage(Proposer node, int msgId) {
		super(node, -1, msgId);
		sender = node;
	}

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_PROPOSER_LEADER_HEARTBEAT));
		bytes.putInt(sender.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_PROPOSER_LEADER_HEARTBEAT;
	}

}
