package ch.usi.inf.paxos.roles;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import ch.usi.inf.logging.Logger;
import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.GeneralNode.NodeType;
import ch.usi.inf.paxos.messages.MessageTimeoutManager;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.acceptor.PaxosPhase1BMessage;
import ch.usi.inf.paxos.messages.acceptor.PaxosPhase2BMessage;
import ch.usi.inf.paxos.messages.client.PaxosClientMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosDecisionMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosPhase1AMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosPhase2AMessage;

public class Proposer extends GeneralNode{

	/* queue of messages received */
	Queue<PaxosMessage> eventArray = new ArrayDeque<PaxosMessage>();
	
	/*
	 * manager to trigger resending of messages when timeout happens
	 * callback on onTimeout 
	 */
	MessageTimeoutManager timeoutManager = new MessageTimeoutManager(this);
	
	/* 
	 * The values proposer holds per slot
	 */
	ConcurrentHashMap<Integer, ValueType> c_vals = new ConcurrentHashMap<Integer, ValueType>();
	ConcurrentHashMap<Integer, Long> c_rnds = new ConcurrentHashMap<Integer, Long>();
	ConcurrentHashMap<Integer, ValueType> decisions = new ConcurrentHashMap<Integer, ValueType>();
	
	/*
	 * The event Cache for each phase per slot
	 */
	ConcurrentHashMap<Integer, PaxosPhase1AMessage> phase1ACaches = new ConcurrentHashMap<Integer, PaxosPhase1AMessage>();
	ConcurrentHashMap<Integer, HashSet<PaxosMessage>> phase1AResponses = new ConcurrentHashMap<Integer, HashSet<PaxosMessage>>();
	ConcurrentHashMap<Integer, PaxosPhase2AMessage> phase2ACaches = new ConcurrentHashMap<Integer, PaxosPhase2AMessage>();
	ConcurrentHashMap<Integer, HashSet<PaxosMessage>> phase2AResponses = new ConcurrentHashMap<Integer, HashSet<PaxosMessage>>();
	
	/*
	 * Leader oracle 
	 */
	LeaderOracle leaderOracle = new LeaderOracle(this);
	/*
	 * singleton for each slot
	 */
	static ConcurrentHashMap<Integer, Proposer> instances = new ConcurrentHashMap<Integer, Proposer>();
	public Proposer(int id, NetworkGroup networkGroup) {
		super(id, networkGroup);
	}
	public static Proposer getById(int id){
		Proposer tmp = new Proposer(id, PaxosConfig.getProposerNetwork());
		Proposer res = instances.putIfAbsent(id, tmp);
		if(res == null)
			return tmp;
		else
			return res;
	}
	
	/*
	 * check timeout for each sent messages (Phase1A, Phase2A), and resend
	 */
	@Override
	public void backgroundLoop(){
		//background thread to broadcast decisions all the time
		new Thread(new DecisionBroadcastThread(this)).start();
		if(PaxosConfig.extraThreadDispatching)
			new Thread(new DispatchThread(this)).start();
		while(true){
			timeoutManager.check();
			try {
				Thread.sleep(PaxosConfig.timeoutCheckInterval);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void eventLoop(){
		while(true){
			PaxosMessage msg = PaxosMessenger.recv(this.getNetworkGroup());
			if(PaxosConfig.extraThreadDispatching)
				eventArray.add(msg);
			else
				dispatchEvent(msg);
		}
	}

	@Override
	public void dispatchEvent(PaxosMessage msg){
		Proposer leader = leaderOracle.getLeader();
		//if(leader == null || leader.getId() != this.getId()){
		if(false){
			
		}else {
			int slot = msg.getSlotIndex();
			switch (msg.getType()){
				case MSG_CLIENT:
					onReceiveClient(msg);
					break;
				case MSG_ACCEPTOR_PHASE1B:
					onReceivePhase1B(msg);
					break;
				case MSG_ACCEPTOR_PHASE2B:
					onReceivePhase2B(msg);
					break;
			}
		}
	}
	
	@Override
	public NodeType getNodeType() {
		return NodeType.PROPOSER;
	}
	
	@Override
	public boolean hasNextEvent(){
		return !eventArray.isEmpty();
	}
	@Override
	public PaxosMessage nextEvent(){
		return eventArray.poll();
	}

	/*
	 * Paxos proposer events 
	 */
	@Override
	public synchronized void onTimeout(PaxosMessage record) {
		int slot = record.getSlotIndex();
		switch (record.getType()){
			case MSG_PROPOSER_PHASE1A:
				if(!phase1FinishedAtThisMoment(record.getSlotIndex()))
					sendPhase1A(record.getSlotIndex());
				break;
			case MSG_PROPOSER_PHASE2A:
				if(!decisions.containsKey(slot))
					sendPhase2A(slot, c_rnds.get(slot), c_vals.get(slot));
				break;
			default:
				break;
		}
	}
	
	public synchronized void sendPhase1A(int slotIndex){
		if(decisions.containsKey(slotIndex)){
			//already decided
			return;
		}
		long c_rnd = incrementAndGetCRnd(slotIndex);
		PaxosPhase1AMessage msg = new PaxosPhase1AMessage(this, slotIndex, c_rnd);
		phase1ACaches.put(slotIndex, msg);
		phase1AResponses.remove(slotIndex);
		timeoutManager.add(msg);
		PaxosMessenger.send(PaxosConfig.getAcceptorNetwork(), msg);
	}
	public synchronized void onReceiveClient(PaxosMessage msg){
		PaxosClientMessage clientMsg = (PaxosClientMessage)msg;
		int slot = msg.getSlotIndex();
		/*
		 * initialize the c_val for this slot, choose the first received client msg
		 */
		ValueType value = clientMsg.getValue();
		ValueType shouldPropose = c_vals.putIfAbsent(slot, value);
		//propose the value of this slot if never proposed
		if(shouldPropose == null) {
			sendPhase1A(clientMsg.getSlotIndex());
		}else {
			//
		}
	}
	public synchronized void onReceivePhase1B(PaxosMessage msg){
		PaxosPhase1BMessage phase1BMsg = (PaxosPhase1BMessage)msg;
		int slot = msg.getSlotIndex();
		if(!phase1ACaches.containsKey(slot)){
			Logger.error("not possible to receive phase1B without having sent phase1A in the leader or this is not the leader");
			return;
		}
		Long c_rnd = c_rnds.get(slot);
		PaxosPhase1AMessage phase1AMsg = phase1ACaches.get(slot);
		if(phase1AMsg.getC_rnd() != c_rnd){
			Logger.error("cached phase1A message mismatches with cached c_rnd");
			return;
		}
		if(phase1BMsg.getRnd() > c_rnd) {
			Logger.error("Not possible phase1B rnd value bigger than leader's c_rnd");
		} else if(phase1BMsg.getRnd() == c_rnd) {
			HashSet<PaxosMessage> tmp = new HashSet<PaxosMessage>();
			HashSet<PaxosMessage> received = phase1AResponses.putIfAbsent(slot, tmp);
			if(received == null)
				received = tmp;
				received.add(phase1BMsg);
				if(gotMajority(received)){
					//possible timeout already happened here
					timeoutManager.remove(phase1AMsg);
					ValueType c_val = null;
					long maxVRand = 0;
					for(PaxosMessage record : received){
						PaxosPhase1BMessage msg1B = (PaxosPhase1BMessage)record;
						if(msg1B.getV_rnd() > maxVRand) {
							maxVRand = msg1B.getV_rnd();
							c_val = msg1B.getV_val();
						}
					}
					if(maxVRand == 0){
						c_val = c_vals.get(slot);
					}else {
						c_vals.put(slot, c_val);
					}
					sendPhase2A(slot, c_rnd, c_val);
				}
		}else {
			//older accept, outdated, ignore
		}
	}
	public synchronized void sendPhase2A(int slotIndex, Long c_rnd, ValueType c_val){
		if(decisions.containsKey(slotIndex)){
			//already decided
			return;
		}
		PaxosPhase2AMessage msg = new PaxosPhase2AMessage(this, slotIndex, c_rnd, c_val);
		phase2ACaches.put(slotIndex, msg);
		phase2AResponses.remove(slotIndex);
		timeoutManager.add(msg);
		PaxosMessenger.send(PaxosConfig.getAcceptorNetwork(), msg);
	}
	public synchronized void onReceivePhase2B(PaxosMessage msg){
		PaxosPhase2BMessage phase2BMsg = (PaxosPhase2BMessage)msg;
		int slot = msg.getSlotIndex();
		if(!phase2ACaches.containsKey(slot)){
			Logger.error("not possible to receive phase2B without having sent phase2A in the leader or this is not the leader");
			return;
		}
		Long c_rnd = c_rnds.get(slot);
		PaxosPhase2AMessage phase2AMsg = phase2ACaches.get(slot);
		if(phase2AMsg.getC_rnd() != c_rnd){
			Logger.error("cached phase2A message mismatches with cached c_rnd");
			return;
		}
		if(phase2BMsg.getV_rnd() < c_rnd){
			//old message, omit
			return;
		}else if(phase2BMsg.getV_rnd() > c_rnd){
			Logger.error("not possible for phase2B message with v-rnd bigger than leader's");
			return;
		}
		HashSet<PaxosMessage> tmp = new HashSet<PaxosMessage>();
		HashSet<PaxosMessage> received = phase2AResponses.putIfAbsent(slot, tmp);
		if(received == null)
			received = tmp;
		received.add(phase2BMsg);
		if(gotMajority(received)){
			timeoutManager.remove(phase2AMsg);
			if(decisions.containsKey(slot) && !decisions.get(slot).equals(phase2BMsg.getV_val())){
				Logger.error("different decision made for slot "+slot+" at proposer "+this.getId());
				return;
			}
			if(!decisions.containsKey(slot)){
				decisions.put(slot, phase2BMsg.getV_val());
				Logger.info("Decision for slot "+slot+" "+ new String(phase2BMsg.getV_val().getValue(), StandardCharsets.UTF_8));
			}
			sendDecision(slot, phase2BMsg.getV_val());
		}
	}
	
	private void sendDecision(int slotIndex, ValueType decision) {
		PaxosDecisionMessage msg = new PaxosDecisionMessage(this, slotIndex, decision);
		PaxosMessenger.send(PaxosConfig.getLearnerNetwork(), msg);
	}

	/* check whether the set of received messages are from NUM_QUORUM of acceptors */
	private boolean gotMajority(HashSet<PaxosMessage> received) {
		HashSet<Integer> acceptorIds = new HashSet<Integer>();
		for(PaxosMessage msg : received){
			acceptorIds.add(msg.getFrom().getId());
		}
		return acceptorIds.size() >= PaxosConfig.NUM_QUORUM;
	}

	/*
	 * Use first 56 bits of nano time + 8 bit of node Id as crnd
	 * it should be very likely increasing and of low chance to be duplicated
	 */
	long lastCRand = 0; //verify incre, for debug only
	synchronized private long incrementAndGetCRnd(int slot) {
		long time = System.nanoTime();
		long res = (time >> 8 << 8) | this.getId();
		c_rnds.put(slot, res);
		if(res <= lastCRand)
			Logger.error("the round number generated is not increasing all the time");
		lastCRand = res;
		return res;
	}
	
	boolean phase1FinishedAtThisMoment(int slot){
		return phase2ACaches.containsKey(slot) && phase2ACaches.get(slot).getC_rnd() == c_rnds.get(slot);
	}
	
	public class LeaderOracle {
		Proposer self;
		LeaderOracle(Proposer self){
			this.self = self;
		}
		
		Proposer leader;
		public void runForLeader(Proposer node){
			
		}
		public Proposer getLeader(){
			return leader;
		}
	}
	
	static class DecisionBroadcastThread implements Runnable{
		Proposer proposer;
		public DecisionBroadcastThread(Proposer proposer) {
			super();
			this.proposer = proposer;
		}
		@Override
		public void run() {
			while(true){
				for(Entry<Integer, ValueType> decision:proposer.decisions.entrySet()){
					proposer.sendDecision(decision.getKey(), decision.getValue());
				}
				try {
					Thread.sleep(PaxosConfig.decisionBroadcastIntervalMilisecs);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
