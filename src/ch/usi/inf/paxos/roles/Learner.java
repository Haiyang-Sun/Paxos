package ch.usi.inf.paxos.roles;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

import ch.usi.inf.logging.Logger;
import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.GeneralNode.NodeType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.proposer.PaxosDecisionMessage;

public class Learner extends GeneralNode {
	ConcurrentHashMap<Integer, ValueType> values = new ConcurrentHashMap<Integer, ValueType>();
	int toOutputSlot = 0;
	static ConcurrentHashMap<Integer, Learner> instances = new ConcurrentHashMap<Integer, Learner>();
	public Learner(int id, NetworkGroup networkGroup) {
		super(id, networkGroup);
	}

	public void onLearnValue(int slot, ValueType value){
		if(slot < toOutputSlot)
			return;
		ValueType existing = values.putIfAbsent(slot, value);
		if(existing != null && !existing.equals(value)){
			Logger.error("receive different decision for slot "+slot);
		}
		while(values.containsKey(toOutputSlot)){
			String res = new String(values.get(toOutputSlot).getValue(), StandardCharsets.UTF_8);
			Logger.info(res);
			toOutputSlot++;
		}
		Logger.debug("waiting for slot "+toOutputSlot+"'s decision");
	}
	
	public static Learner getById(int id){
		Learner tmp = new Learner(id, PaxosConfig.getLearnerNetwork());
		Learner res = instances.putIfAbsent(id, tmp);
		if(res == null)
			return tmp;
		else
			return res;
	}
	
	@Override
	public void eventLoop(){
		while(true){
			PaxosMessage msg = PaxosMessenger.recv(this.getNetworkGroup());
			dispatchEvent(msg);
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
