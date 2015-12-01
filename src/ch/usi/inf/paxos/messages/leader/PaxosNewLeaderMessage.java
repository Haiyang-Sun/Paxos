package ch.usi.inf.paxos.messages.leader;

import java.nio.ByteBuffer;

import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.PaxosMessenger.MessageType;
import ch.usi.inf.paxos.roles.Acceptor;
import ch.usi.inf.paxos.roles.LeaderOracle;
import ch.usi.inf.paxos.roles.LeaderOracle.Leader;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosNewLeaderMessage extends PaxosMessage {
	private Acceptor acceptor;
	private LeaderOracle.Leader leader;
	public LeaderOracle.Leader getLeader() {
		return leader;
	}

	public PaxosNewLeaderMessage(Acceptor sender, Proposer leader, int generation) {
		/*
		 * TODO, separte leader messages from paxos messages
		 */
		super(sender , -1);
		this.acceptor = sender;
		this.leader = new Leader(leader, generation);
	}
	
	public PaxosNewLeaderMessage(Acceptor sender, Proposer leader, int generation, int msgId) {
		super(sender, -1, msgId);
		this.acceptor = sender;
		this.leader = new Leader(leader, generation);
	}
	

	@Override
	public ByteBuffer getMessageBytes(){
		ByteBuffer bytes = ByteBuffer.allocate(PaxosMessenger.MAX_PACKET_LENGTH);
		bytes.put(PaxosMessenger.msgType2Byte(MessageType.MSG_ACCEPTOR_CURRENT_LEADER));
		bytes.putInt(acceptor.getId());
		bytes.putInt(slotIndex);
		bytes.putInt(msgId);
		bytes.putInt(leader.getLeaderNode().getId());
		bytes.putInt(leader.getGeneration());
		return bytes;
	}
	
	@Override
	public MessageType getType(){
		return MessageType.MSG_ACCEPTOR_CURRENT_LEADER;
	}
}
