package ch.usi.inf.paxos.roles;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

import ch.usi.inf.logging.Logger;
import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.message.learner.PaxosLearnMessage;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.leader.PaxosPushMessage;

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

		//ask for decided values
		if (toOutputSlot < slot){
			for (int start = toOutputSlot; start < slot; start++){
				if (!values.containsKey(start)){
					sendLearnMessage(start);
				}
			}
			
		}

		if(existing != null && !existing.equals(value)){
			//TODO: actually possible. Leader change.
			Logger.error("receive different decision for slot "+slot);
		}
		while(values.containsKey(toOutputSlot)){
			String res = new String(values.get(toOutputSlot).getValue(), StandardCharsets.UTF_8);
			Logger.info(res);
			toOutputSlot++;
		}
		Logger.debug("waiting for slot "+toOutputSlot+"'s decision");
	}
	
	private synchronized void sendLearnMessage(int slot) {
		PaxosLearnMessage msg = new PaxosLearnMessage(this, slot, slot);
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), msg);		
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
	
	@Override
	public void backgroundLoop(){
		new Thread(new LearnTimer(this)).start();
	}
	
	class LearnTimer implements Runnable{
		private Learner learner;
		public LearnTimer(Learner learner){
			this.learner = learner;
		}
		@Override
		public void run() {
			learner.sendLearnMessage(learner.getOutputSlot());
			try {
				Thread.sleep(PaxosConfig.learnerFetchInterval);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	
	public void dispatchEvent(PaxosMessage msg){
		int slot = msg.getSlotIndex();
		switch (msg.getType()){
			case MSG_PROPOSER_PUSH:
				PaxosPushMessage decisionMsg = (PaxosPushMessage) msg;
				onLearnValue(slot, decisionMsg.getDecision());
				break;
		}
	}
	
	@Override
	public NodeType getNodeType() {
		return NodeType.LEARNER;
	}
	
	public int getOutputSlot(){
		return toOutputSlot;
	}
}
