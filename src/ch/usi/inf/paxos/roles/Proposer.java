package ch.usi.inf.paxos.roles;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import ch.usi.inf.logging.Logger;
import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.message.learner.PaxosLearnMessage;
import ch.usi.inf.paxos.messages.MessageTimeoutManager;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.acceptor.PaxosPhase1BMessage;
import ch.usi.inf.paxos.messages.acceptor.PaxosPhase2BMessage;
import ch.usi.inf.paxos.messages.client.PaxosClientMessage;
import ch.usi.inf.paxos.messages.leader.PaxosAskForLeaderMessage;
import ch.usi.inf.paxos.messages.leader.PaxosClientSuccessMessage;
import ch.usi.inf.paxos.messages.leader.PaxosLeaderHeartBeatMessage;
import ch.usi.inf.paxos.messages.leader.PaxosNewLeaderMessage;
import ch.usi.inf.paxos.messages.leader.PaxosProposerHeartBeatMessage;
import ch.usi.inf.paxos.messages.leader.PaxosPushMessage;
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

	//local variables: changed to a single process instead of multi slot --by Rui
	AtomicInteger proposerCurSlot = new AtomicInteger(0);
	CopyOnWriteArrayList<ValueType> clientAcceptedList = new CopyOnWriteArrayList<ValueType>();
	ConcurrentHashMap<Integer, Integer> clientAcceptedSlot = new ConcurrentHashMap<Integer, Integer>();
	
	private static AtomicBoolean idle = new AtomicBoolean(true);
	private static AtomicBoolean escapePhase1 = new AtomicBoolean(false);

	private static AtomicLong c_rnd_prefix = new AtomicLong(0);
	private static AtomicLong c_rnd = new AtomicLong(0);
	private static ValueType c_val = ValueType.NIL;

	//for leader election
	final static AtomicInteger suspectCnt = new AtomicInteger(0);
	private static AtomicInteger currentLeaderID = new AtomicInteger(0);
	private static HashSet<Integer> proposerIDPool = new HashSet<Integer>();
	

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
		//new Thread(new DecisionBroadcastThread(this)).start();
		new Thread(new LeaderTimer(this)).start();
		new Thread(new Phase1AThread(this)).start();
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
//		if(leaderOracle.getLeader() == null){
//			leaderOracle.runForLeader();
//		}
//		if(!leaderOracle.selfIsLeader()){
//		if(false){

		//do nothing but just maintain the heartbeat
		if(!isLeader()){
			switch (msg.getType()){
			case MSG_PROPOSER_DECIDE:
					onReceiveDecide(msg);
					break;
				case MSG_PROPOSER_LEADER_HEARTBEAT:
					onReceiveLeaderHeartBeat(msg);
					break;
				case MSG_PROPOSER_HEARTBEAT:
					onReceiveProposerHeartBeat(msg);
					break;
			}
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
				case MSG_LEARNER_LEARN:
					onReceiveLearn(msg);
					break;
				case MSG_ACCEPTOR_ASK_FOR_LEADER:
					leaderOracle.onAskForLeader((PaxosAskForLeaderMessage) msg);
					break;
				case MSG_ACCEPTOR_CURRENT_LEADER:
					leaderOracle.onReceiveLeaderInfo((PaxosNewLeaderMessage) msg);
					break;
				case MSG_PROPOSER_LEADER_HEARTBEAT:
					onReceiveLeaderHeartBeat(msg);
					break;
				case MSG_PROPOSER_HEARTBEAT:
					onReceiveProposerHeartBeat(msg);
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

		//slot has been proposed
		if (slot != this.proposerCurSlot.get()){
			return;
		}

		switch (record.getType()){
			case MSG_PROPOSER_PHASE1A:
				if(!phase1FinishedAtThisMoment(record.getSlotIndex()))
					sendPhase1A();
				break;
			case MSG_PROPOSER_PHASE2A:
				if(!decisions.containsKey(slot))
					sendPhase2A(slot, c_rnd.get(), c_val);
				break;
			default:
				break;
		}
	}
	
	public synchronized void sendPhase1A(){
		c_rnd.set(Long.parseLong(String.format("%08d", c_rnd_prefix.get()) + String.format("%03d", id)));
		PaxosPhase1AMessage msg = new PaxosPhase1AMessage(this, proposerCurSlot.get(), c_rnd.get());
		phase1ACaches.put(proposerCurSlot.get(), msg);
		timeoutManager.add(msg);
		PaxosMessenger.send(PaxosConfig.getAcceptorNetwork(), msg);
	}

	public synchronized void onReceiveClient(PaxosMessage msg){
		if (!isLeader()){
			return;
		}
		PaxosClientMessage clientMsg = (PaxosClientMessage)msg;
		int clientId = clientMsg.getFrom().getId();
		int clientSlot = msg.getSlotIndex();
		if (!clientAcceptedSlot.containsKey(clientId)){
			clientAcceptedSlot.put(clientId, -1);
		}
		//has been previously processed, skip
		if (clientAcceptedSlot.get(clientId) >= clientSlot){
			return;
		}

		clientAcceptedSlot.put(clientId, clientSlot);
		Logger.debug("Received slot " + clientSlot + " from client " + clientId);
		ValueType value = clientMsg.getValue();
		clientAcceptedList.add(value);
		sendClientSuccess(clientId, clientSlot);
//		int slot = clientAcceptedList.size() - 1;
//		sendClientSuccess(clientId, clientSlot);
//		ValueType shouldPropose = c_vals.putIfAbsent(slot, value);
	}
	public synchronized void onReceivePhase1B(PaxosMessage msg){
		if (idle.get() == true)
			return;

		PaxosPhase1BMessage phase1BMsg = (PaxosPhase1BMessage)msg;
		int slot = msg.getSlotIndex();
//		if(!phase1ACaches.containsKey(slot)){
//			Logger.error("not possible to receive phase1B without having sent phase1A in the leader or this is not the leader");
//			return;
//		}

		//if slot has been processed
		if (slot > proposerCurSlot.get()){
			proposerCurSlot.set(slot);
			idle.set(true);
			return;
		}
		//if it is Phase1B from previous round
		if (slot < proposerCurSlot.get()){
			return;
		}

		PaxosPhase1AMessage phase1AMsg = phase1ACaches.get(slot);
//		if(phase1AMsg.getC_rnd() != c_rnd){
//			Logger.error("cached phase1A message mismatches with cached c_rnd");
//			return;
//		}
//		if(phase1BMsg.getRnd() > c_rnd) {
//			Logger.error("Not possible phase1B rnd value bigger than leader's c_rnd");
//		} else if(phase1BMsg.getRnd() == c_rnd) {
		if(phase1BMsg.getRnd() == c_rnd.get()) {
			HashSet<PaxosMessage> tmp = new HashSet<PaxosMessage>();
			HashSet<PaxosMessage> received = phase1AResponses.putIfAbsent(slot, tmp);
			if(received == null)
				received = tmp;

			received.add(phase1BMsg);
			if(gotMajority(received)){
				//possible timeout already happened here
				timeoutManager.remove(phase1AMsg);
				ValueType c_val = ValueType.NIL;
				long maxVRand = 0;
				for(PaxosMessage record : received){
					PaxosPhase1BMessage msg1B = (PaxosPhase1BMessage)record;
					if(msg1B.getV_rnd() > maxVRand) {
						maxVRand = msg1B.getV_rnd();
						c_val = msg1B.getV_val();
					}
				}
				if(maxVRand == 0){
					this.c_val = clientAcceptedList.get(proposerCurSlot.get());
					c_val = this.c_val;
				}else {
					this.c_val = c_val;
				}
				sendPhase2A(slot, c_rnd.get(), c_val);
			}
		}else if (phase1BMsg.getRnd() > c_rnd.get()){
			//other leader has taken part in. Increase the c_rnd
			increaseAndResendPhase1A();
		}
	}

	private void increaseAndResendPhase1A() {
		c_rnd_prefix.incrementAndGet();
		sendPhase1A();
	}

	public synchronized void sendPhase2A(int slotIndex, Long c_rnd, ValueType c_val){
		if(decisions.containsKey(slotIndex)){
			//already decided
			return;
		}
		boolean escapeFlag = PaxosConfig.escapePhase1 & escapePhase1.get();
		PaxosPhase2AMessage msg = new PaxosPhase2AMessage(this, slotIndex, c_rnd, c_val, escapeFlag);
		phase2ACaches.put(slotIndex, msg);
		phase2AResponses.remove(slotIndex);
		timeoutManager.add(msg);
		PaxosMessenger.send(PaxosConfig.getAcceptorNetwork(), msg);
	}
	public synchronized void onReceivePhase2B(PaxosMessage msg){
		if (idle.get() == true)
			return;

		PaxosPhase2BMessage phase2BMsg = (PaxosPhase2BMessage)msg;
		int slot = msg.getSlotIndex();

//		if(!phase2ACaches.containsKey(slot)){
//			Logger.error("not possible to receive phase2B without having sent phase2A in the leader or this is not the leader");
//			return;
//		}

		//if slot has been processed
		if (slot > proposerCurSlot.get()){
			proposerCurSlot.set(slot);
			idle.set(true);
			return;
		}
		//if it is Phase1B from previous round
		if (slot < proposerCurSlot.get()){
			return;
		}

		Long c_rnd = this.c_rnd.get();
		if (phase2BMsg.getEscapePhase1() == false){
			escapePhase1.set(false);
		}

		PaxosPhase2AMessage phase2AMsg = phase2ACaches.get(slot);

//		if(phase2AMsg.getC_rnd() != c_rnd){
//			Logger.error("cached phase2A message mismatches with cached c_rnd");
//			return;
//		}
		if(phase2BMsg.getV_rnd() < c_rnd){
			//old message, omit
			return;
		}else if(phase2BMsg.getV_rnd() > c_rnd){
			Logger.debug("ger reject from accpetors. Start over.");
			increaseAndResendPhase1A();
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
				Logger.error("different decision made for slot "+slot+" at proposer "+ this.getId());
				return;
			}
			if(!decisions.containsKey(slot)){
				decisions.put(slot, phase2BMsg.getV_val());
				Logger.info("Decision for slot "+slot+" "+ new String(phase2BMsg.getV_val().getValue(), StandardCharsets.UTF_8));
				proposerCurSlot.incrementAndGet();
				idle.set(true);
			}
			sendDecision(slot, phase2BMsg.getV_val());
			sendPush(slot, phase2BMsg.getV_val());
			if (PaxosConfig.escapePhase1){
				this.escapePhase1.set(true);
			}
			Logger.debug("move to slot: " + proposerCurSlot.get());
		}
	}

	private synchronized void onReceiveLearn(PaxosMessage msg) {
		PaxosLearnMessage learnMsg = (PaxosLearnMessage)msg;
		int slot = learnMsg.getRequireSlot();
		if (proposerCurSlot.get() >= slot){
			sendPush(slot, clientAcceptedList.get(slot));
		}
	}

	private synchronized void onReceiveDecide(PaxosMessage msg) {
		PaxosDecisionMessage decMsg = (PaxosDecisionMessage)msg;
		if (!isLeader()){
			decisions.put(decMsg.getSlotIndex(), decMsg.getDecision());
			proposerCurSlot.set(decMsg.getSlotIndex());
		}
	}
				

	private synchronized void onReceiveProposerHeartBeat(PaxosMessage msg) {
		PaxosProposerHeartBeatMessage proposerHeartBeatMsg = (PaxosProposerHeartBeatMessage)msg;
		int proposerID = proposerHeartBeatMsg.getFrom().getId();
		Logger.heartbeat("received proposer Heartbeat:" + proposerID);
		proposerIDPool.add(proposerID);
	}

	private synchronized void onReceiveLeaderHeartBeat(PaxosMessage msg) {
		PaxosLeaderHeartBeatMessage leaderHeartBeatMsg = (PaxosLeaderHeartBeatMessage)msg;
		int leaderID = leaderHeartBeatMsg.getFrom().getId();
		if (leaderID != currentLeaderID.get()) {
			proposerIDPool.add(leaderID);
		} else{
			suspectCnt.set(0);
		}
	}

	private void sendClientSuccess(int clientId, int slotIndex) {
		PaxosClientSuccessMessage msg = new PaxosClientSuccessMessage(this, clientId, slotIndex);
		PaxosMessenger.send(PaxosConfig.getClientNetwork(), msg);
	}
	
	private void sendDecision(int slotIndex, ValueType decision) {
		PaxosDecisionMessage msg = new PaxosDecisionMessage(this, slotIndex, decision);
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), msg);
	}

	private void sendPush(int slotIndex, ValueType decision) {
		PaxosPushMessage msg = new PaxosPushMessage(this, slotIndex, decision);
		PaxosMessenger.send(PaxosConfig.getLearnerNetwork(), msg);
	}
	
	private void sendLeaderHB(){
		PaxosLeaderHeartBeatMessage msg = new PaxosLeaderHeartBeatMessage(this);
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), msg);
	}

	private void sendProposerHB(){
		PaxosProposerHeartBeatMessage msg = new PaxosProposerHeartBeatMessage(this);
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), msg);
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
		//return phase2ACaches.containsKey(slot) && phase2ACaches.get(slot).getC_rnd() == c_rnds.get(slot);
		return phase2ACaches.containsKey(slot) && slot <= proposerCurSlot.get();
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
	static class Phase1AThread implements Runnable{
		Proposer proposer;
		public Phase1AThread(Proposer proposer) {
			super();
			this.proposer = proposer;
		}
		@Override
		public void run() {
			while (true){
				if (proposer.isIdle() && !proposer.listIsEmpty()){
					proposer.setIdle(false);
					proposer.startNewRound();
				}
			}
			
		}
	}
	
	public void startNewRound(){
		if (PaxosConfig.escapePhase1 & this.escapePhase1.get()){
				c_val = clientAcceptedList.get(proposerCurSlot.get());
				Logger.debug("Escape phase 1A");
				sendPhase2A(proposerCurSlot.get(), c_rnd.get(), c_val);
		} else {
			sendPhase1A();
		}
	}
	
	public boolean listIsEmpty(){
		return clientAcceptedList.size() <= proposerCurSlot.get();
	}
	
	public boolean isIdle(){
		return idle.get();
	}
	
	public void setIdle(boolean flag){
		idle.set(flag);
	}

	//used for keeping heartbeat among leaders and proposers
	static class LeaderTimer implements Runnable{
		Proposer proposer;
		public LeaderTimer(Proposer proposer) {
			super();
			this.proposer = proposer;
		}
		@Override
		public void run() {
			while(true){
				proposerIDPool.add(proposer.getId());

				//if there is a proposer with higher id, make it the leader
				if (proposer.isLeader() == true &&
						Collections.max(proposerIDPool) > proposer.id){
					Logger.debug("Current Leader " + proposer.getId() + " found a propser with smaller id. Changing Leader.");
					proposer.changeLeader();
				}

				//if the current leader loses its heartbeat, change the leader
				if (proposer.isLeader() == false &&
						suspectCnt.getAndIncrement() > PaxosConfig.susptIntervalCnt){
					Logger.debug("Lose leader heartbeat. Changing leader.");
					proposer.changeLeader();
				}
				
				if (proposer.isLeader()){
					proposer.sendLeaderHB();
				}
				
				proposer.sendProposerHB();

				try {
					Thread.sleep(PaxosConfig.proposerInterval);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private boolean isLeader(){
		return currentLeaderID.get() == id;
	}

	synchronized private void changeLeader(){
		int id = currentLeaderID.get();
		proposerIDPool.remove(id);
		int newLeader = Collections.max(proposerIDPool);
		currentLeaderID.set(newLeader);
		Logger.debug("Leader has been set to " + newLeader);
	}
}
