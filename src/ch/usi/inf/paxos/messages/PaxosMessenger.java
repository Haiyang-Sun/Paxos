package ch.usi.inf.paxos.messages;

import java.nio.ByteBuffer;
import java.util.Random;

import ch.usi.inf.logging.Logger;
import ch.usi.inf.network.BaseMulticast;
import ch.usi.inf.network.Multicast;
import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.PaxosConfig.NetworkLevel;
import ch.usi.inf.paxos.messages.acceptor.PaxosPhase1BMessage;
import ch.usi.inf.paxos.messages.acceptor.PaxosPhase2BMessage;
import ch.usi.inf.paxos.messages.client.PaxosClientMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosDecisionMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosPhase1AMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosPhase2AMessage;
import ch.usi.inf.paxos.roles.Acceptor;
import ch.usi.inf.paxos.roles.Client;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosMessenger {
	
	public static int MAX_PACKET_LENGTH = 50000;
	
	public enum MessageType{MSG_CLIENT, MSG_PROPOSER_PHASE1A, MSG_PROPOSER_PHASE2A,MSG_ACCEPTOR_PHASE1B,MSG_ACCEPTOR_PHASE2B, MSG_PROPOER_DECIDE, MSG_UNKONWN};
	
	public static byte msgType2Byte(MessageType type){
		switch(type){
			case MSG_CLIENT:
				return 0;
			case MSG_PROPOSER_PHASE1A:
				return 1;
			case MSG_ACCEPTOR_PHASE1B:
				return 2;
			case MSG_PROPOSER_PHASE2A:
				return 3;
			case MSG_ACCEPTOR_PHASE2B:
				return 4;
			case MSG_PROPOER_DECIDE:
				return 5;
		}
		return -1;
	}
	
	public static MessageType byte2MsgType(byte type){
		switch(type){
			case 0:
				return MessageType.MSG_CLIENT;
			case 1:
				return MessageType.MSG_PROPOSER_PHASE1A;
			case 2:
				return MessageType.MSG_ACCEPTOR_PHASE1B;
			case 3:
				return MessageType.MSG_PROPOSER_PHASE2A;
			case 4:
				return MessageType.MSG_ACCEPTOR_PHASE2B;
			case 5:
				return MessageType.MSG_PROPOER_DECIDE;
		}
		return MessageType.MSG_UNKONWN;
	}
	
	static Multicast getMulticast(NetworkGroup target){
		if(PaxosConfig.getNetworkLevel() == NetworkLevel.NORMAL)
			return new BaseMulticast(target);
		else
			return null;
	}
	
	public static void send(NetworkGroup target, PaxosMessage msg){
		randomSleep(PaxosConfig.randomSleep);
		Logger.debug("Send message "+msg.toString()+" to "+target);
		getMulticast(target).send(msg.getMessageBytes());
	}
	
	public static PaxosMessage recv(NetworkGroup self){
		return dispatch(getMulticast(self).receive(MAX_PACKET_LENGTH));
	}
	
	public static PaxosMessage dispatch(ByteBuffer buf){
		/*
		 * Messages should share a common header
		 * 0 - msg type
		 * 1 - 4  node id
		 * 5 - 8  slot id 
		 * 9 - 12 msg id
		 * 13 - value
		 */
		int size = buf.position();
		buf.rewind();
		byte msgType = buf.get();
		int nodeId = buf.getInt();
		int slotIndex = buf.getInt();
		int msgId = buf.getInt();
		
		
		
		MessageType type = PaxosMessenger.byte2MsgType(msgType);
		byte []valueBuf;
		
		PaxosMessage res = null;
		int position;
		
		long v1,v2;
		switch(type){
			case MSG_CLIENT:
				position = buf.position();
				valueBuf = new byte[size - position];
				buf.get(valueBuf, 0, size - position);
				res = new PaxosClientMessage(Client.getById(nodeId), new ValueType(valueBuf), slotIndex, msgId);
				break;
			case MSG_PROPOSER_PHASE1A:
				res = new PaxosPhase1AMessage(Proposer.getById(nodeId), slotIndex, buf.getLong(), msgId);
				break;
			case MSG_ACCEPTOR_PHASE1B:
				v1 = buf.getLong();
				v2 = buf.getLong();
				position = buf.position();
				valueBuf = new byte[size - position];
				buf.get(valueBuf, 0, size - position);
				res = new PaxosPhase1BMessage(Acceptor.getById(nodeId), slotIndex, v1 ,v2, new ValueType(valueBuf), msgId);
				break;
			case MSG_PROPOSER_PHASE2A:
				v1 = buf.getLong();
				position = buf.position();
				valueBuf = new byte[size - position];
				buf.get(valueBuf, 0, size - position);
				res = new PaxosPhase2AMessage(Proposer.getById(nodeId), slotIndex, v1, new ValueType(valueBuf), msgId);
				break;
			case MSG_ACCEPTOR_PHASE2B:
				v1 = buf.getLong();
				position = buf.position();
				valueBuf = new byte[size - position];
				buf.get(valueBuf, 0, size - position);
				res = new PaxosPhase2BMessage(Acceptor.getById(nodeId), slotIndex, v1, new ValueType(valueBuf), msgId);
				break;
			case MSG_PROPOER_DECIDE:
				position = buf.position();
				valueBuf = new byte[size - position];
				buf.get(valueBuf, 0, size - position);
				res = new PaxosDecisionMessage(Proposer.getById(nodeId), slotIndex, new ValueType(valueBuf), msgId);
				break;
			case MSG_UNKONWN:
				Logger.error("Unknown message is received");
		}
		Logger.debug("Received message "+res.toString());
		return res;
	}
	
	private static void randomSleep(int miliSec){
		//set acceptor start time so as to avoid broadcast at the same pace; 
		Random random = new Random(System.currentTimeMillis());
		try {
			Thread.sleep(random.nextInt(miliSec));
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}