package ch.usi.inf.paxos.roles;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.GeneralNode.NodeType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.proposer.PaxosDecisionMessage;

public class Learner extends GeneralNode {
	public Learner(int id, NetworkGroup networkGroup) {
		super(id, networkGroup);
	}

	ConcurrentHashMap<Integer, ValueType> values = new ConcurrentHashMap<Integer, ValueType>();
	int toOutputSlot = 0;
	public void onLearnValue(int slot, ValueType value){
		ValueType existing = values.putIfAbsent(slot, value);
		if(existing != null && !existing.equals(value)){
			System.err.println("receive different decision for slot "+slot);
		}
		while(values.containsKey(toOutputSlot)){
			System.out.println(values.get(toOutputSlot));
			toOutputSlot++;
		}
		if(PaxosConfig.debug)
			System.out.println("waiting for slot "+toOutputSlot+"'s decision");
	}
	
	static ConcurrentHashMap<Integer, Learner> instances = new ConcurrentHashMap<Integer, Learner>();  
	
	public static Learner getById(int id){
		Learner tmp = new Learner(id, PaxosConfig.getLearnerNetwork());
		Learner res = instances.putIfAbsent(id, tmp);
		if(res == null)
			return tmp;
		else
			return res;
	}
	
	HashSet<PaxosMessage> events = new HashSet<PaxosMessage>();
	@Override
	public void eventLoop(){
		while(true){
			PaxosMessage msg = PaxosMessenger.recv(this.getNetworkGroup());
			if(!events.contains(msg)){
				events.add(msg);
				dispatchEvent(msg);
			}
		}
	}
	
	public void dispatchEvent(PaxosMessage msg){
		int slot = msg.getSlotIndex();
		switch (msg.getType()){
			case MSG_PROPOER_DECIDE:
				PaxosDecisionMessage decisionMsg = (PaxosDecisionMessage) msg;
				onLearnValue(slot, decisionMsg.getDecision());
				break;
		}
	}
	
	@Override
	public NodeType getNodeType() {
		return NodeType.LEARNER;
	}
}
