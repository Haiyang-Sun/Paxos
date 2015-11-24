package ch.usi.inf.paxos.roles;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.GeneralNode.DispatchThread;
import ch.usi.inf.paxos.GeneralNode.NodeType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.acceptor.PaxosPhase1BMessage;
import ch.usi.inf.paxos.messages.acceptor.PaxosPhase2BMessage;
import ch.usi.inf.paxos.messages.client.PaxosClientMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosPhase1AMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosPhase2AMessage;
import ch.usi.inf.paxos.roles.Proposer.LeaderOracle;

public class Acceptor extends GeneralNode{
	ConcurrentHashMap<Integer, Long> rnds = new ConcurrentHashMap<Integer, Long>();
	ConcurrentHashMap<Integer, Long> v_rnds = new ConcurrentHashMap<Integer, Long>();
	ConcurrentHashMap<Integer, ValueType> v_vals = new ConcurrentHashMap<Integer, ValueType>();
	static ConcurrentHashMap<Integer, Acceptor> instances = new ConcurrentHashMap<Integer, Acceptor>();
	//HashSet<PaxosMessage> events = new HashSet<PaxosMessage>();
	Queue<PaxosMessage> eventArray = new ArrayDeque<PaxosMessage>();
	
	public Acceptor(int id, NetworkGroup networkGroup, boolean realInstance) {
		super(id, networkGroup);
		if(realInstance && PaxosConfig.extraThreadDispatching)
			new Thread(new DispatchThread(this)).start();
	}
	
	
	public static Acceptor getById(int id){
		return getById(id, false);
	}
	public static Acceptor getById(int id, boolean realInstance){
		Acceptor tmp = new Acceptor(id, PaxosConfig.getAcceptorNetwork(), realInstance);
		Acceptor res = instances.putIfAbsent(id, tmp);
		if(res == null)
			return tmp;
		else
			return res;
	}
	
	public synchronized void onReceivePhase1A(PaxosMessage msg){
		PaxosPhase1AMessage phase1AMsg = (PaxosPhase1AMessage) msg;
		int slot = msg.getSlotIndex();
		rnds.putIfAbsent(slot, 0L);
		v_rnds.putIfAbsent(slot, 0L);
		v_vals.putIfAbsent(slot, ValueType.NIL);
		if(phase1AMsg.getC_rnd() > rnds.get(slot)){
			rnds.put(slot, phase1AMsg.getC_rnd());
			sendPhase1B(slot,  rnds.get(slot), v_rnds.get(slot), v_vals.get(slot));
		}
	}
	public synchronized void sendPhase1B(int slotIndex, Long rnd, Long v_rnd, ValueType v_val){
		PaxosPhase1BMessage msg = new PaxosPhase1BMessage(this, slotIndex, rnd, v_rnd, v_val);
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), msg);
	}
	public synchronized void onReceivePhase2A(PaxosMessage msg){
		PaxosPhase2AMessage phase2AMsg = (PaxosPhase2AMessage) msg;
		int slot = msg.getSlotIndex();
		//in case an acceptor is started half-way
		rnds.putIfAbsent(slot, 0L);
		v_rnds.putIfAbsent(slot, 0L);
		v_vals.putIfAbsent(slot, ValueType.NIL);
		
		if(phase2AMsg.getC_rnd() >= rnds.get(slot)){
			v_rnds.put(slot, phase2AMsg.getC_rnd());
			v_vals.put(slot, phase2AMsg.getC_val());
			sendPhase2B(slot, v_rnds.get(slot), v_vals.get(slot));
		}
	}
	public synchronized void sendPhase2B(int slotIndex, Long v_rnd, ValueType v_val){
		PaxosPhase2BMessage msg = new PaxosPhase2BMessage(this, slotIndex, v_rnd, v_val);
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), msg);
	}
	
	@Override
	public void eventLoop(){
		while(true){
			PaxosMessage msg = PaxosMessenger.recv(this.getNetworkGroup());
			//if(!events.contains(msg)){
			//	events.add(msg);
			if(PaxosConfig.extraThreadDispatching)
				eventArray.add(msg);
			else
				dispatchEvent(msg);
			//}
		}
	}
	@Override
	public void dispatchEvent(PaxosMessage msg){
			switch (msg.getType()){
				case MSG_PROPOSER_PHASE1A:
					onReceivePhase1A(msg);
					break;
				case MSG_PROPOSER_PHASE2A:
					onReceivePhase2A(msg);
					break;
			}
	}
	
	@Override
	public NodeType getNodeType() {
		return NodeType.ACCEPTOR;
	}
	
	@Override
	public boolean hasNextEvent(){
		return !eventArray.isEmpty();
	}
	@Override
	public PaxosMessage nextEvent(){
		return eventArray.poll();
	}
}
