package ch.usi.inf.paxos.roles;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.omg.PortableServer.POA;

import ch.usi.inf.logging.Logger;
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

public class Acceptor extends GeneralNode{
	ConcurrentHashMap<Integer, Long> rnds = new ConcurrentHashMap<Integer, Long>();
	ConcurrentHashMap<Integer, Long> v_rnds = new ConcurrentHashMap<Integer, Long>();
	ConcurrentHashMap<Integer, ValueType> v_vals = new ConcurrentHashMap<Integer, ValueType>();
	static ConcurrentHashMap<Integer, Acceptor> instances = new ConcurrentHashMap<Integer, Acceptor>();
	//HashSet<PaxosMessage> events = new HashSet<PaxosMessage>();
	Queue<PaxosMessage> eventArray = new ArrayDeque<PaxosMessage>();
	
	//local variable after switching to a "pipeline" style
	AtomicInteger maxSlot = new AtomicInteger(-1);
	private long rnd = 0; 
	private long v_rnd = 0;
	private ValueType v_val = ValueType.NIL;
	
	private AtomicBoolean escapeCheck = new AtomicBoolean(false);
	private AtomicInteger lastProposalId = new AtomicInteger(0);
	
	public Acceptor(int id, NetworkGroup networkGroup) {
		super(id, networkGroup);
	}
	
	@Override
	public void backgroundLoop(){
		if(PaxosConfig.extraThreadDispatching)
			new Thread(new DispatchThread(this)).start();
	}
	
	public static Acceptor getById(int id){
		Acceptor tmp = new Acceptor(id, PaxosConfig.getAcceptorNetwork());
		Acceptor res = instances.putIfAbsent(id, tmp);
		if(res == null)
			return tmp;
		else
			return res;
	}
	
	public synchronized void onReceivePhase1A(PaxosMessage msg){
		PaxosPhase1AMessage phase1AMsg = (PaxosPhase1AMessage) msg;
		int slot = msg.getSlotIndex();
		Logger.debug("received phase1A from proposer " + msg.getFrom().getId() + 
				" for slot " + slot +
				" with c_rnd " + phase1AMsg.getC_rnd());
		//current slot
		if (slot == maxSlot.get()){
			checkEscape1A(phase1AMsg.getFrom().getId());
			if(phase1AMsg.getC_rnd() > rnd){
				rnd = phase1AMsg.getC_rnd(); 
				v_rnd = 0;
				v_val = ValueType.NIL;
			}
		}
		//newer slot, which means the previous ones have been decided
		if (slot > maxSlot.get()){
			maxSlot.set(slot);
			rnd = phase1AMsg.getC_rnd(); 
			v_rnd = 0;
			v_val = ValueType.NIL;
		}
		sendPhase1B(maxSlot.get(),  rnd, v_rnd, v_val);

//		rnds.putIfAbsent(slot, 0L);
//		v_rnds.putIfAbsent(slot, 0L);
//		v_vals.putIfAbsent(slot, ValueType.NIL);
//		if(phase1AMsg.getC_rnd() > rnds.get(slot)){
//			rnds.put(slot, phase1AMsg.getC_rnd());
//			sendPhase1B(slot,  rnds.get(slot), v_rnds.get(slot), v_vals.get(slot));
//		}
	}
	public synchronized void sendPhase1B(int slotIndex, Long rnd, Long v_rnd, ValueType v_val){
		PaxosPhase1BMessage msg = new PaxosPhase1BMessage(this, slotIndex, rnd, v_rnd, v_val);
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), msg);
	}
	public synchronized void onReceivePhase2A(PaxosMessage msg){
		PaxosPhase2AMessage phase2AMsg = (PaxosPhase2AMessage) msg;
		int slot = msg.getSlotIndex();
		boolean escape1AFlag = PaxosConfig.escapePhase1 & phase2AMsg.getEscapePhase1();

		checkEscape2A(phase2AMsg.getFrom().getId(), phase2AMsg.getEscapePhase1());
		//not current slot, ignore
		if (slot != maxSlot.get()){
			if (!(escape1AFlag))
				return;
			else{
				maxSlot.set(slot);
			}
		}

		//in case an acceptor is started half-way
		//rnds.putIfAbsent(slot, 0L);
		//v_rnds.putIfAbsent(slot, 0L);
		//v_vals.putIfAbsent(slot, ValueType.NIL);
		if (escape1AFlag){
			Logger.debug("Receive a 2AMsg without 1A, proceed");
			v_rnd = phase2AMsg.getC_rnd();
			v_val = phase2AMsg.getC_val();
			sendPhase2B(slot, v_rnd, v_val, this.escapeCheck.get());
			return;
		}
		
		if(phase2AMsg.getC_rnd() >= rnd){
//			v_rnds.put(slot, phase2AMsg.getC_rnd());
//			v_vals.put(slot, phase2AMsg.getC_val());
			v_rnd = phase2AMsg.getC_rnd();
			v_val = phase2AMsg.getC_val();
			sendPhase2B(slot, v_rnd, v_val, escape1AFlag & this.escapeCheck.get());
		} else {
			//TODO: add reject message
			sendPhase2B(slot, v_rnd, v_val, false);
		}
	}
	public synchronized void sendPhase2B(int slotIndex, Long v_rnd, ValueType v_val, boolean flag){
		PaxosPhase2BMessage msg = new PaxosPhase2BMessage(this, slotIndex, v_rnd, v_val, flag & escapeCheck.get());
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), msg);
	}
	
	public void checkEscape1A(int proposerId){
		if (lastProposalId.get() == proposerId){
			//remain it
		} else {
			escapeCheck.set(false);
			lastProposalId.set(0);
		}
	}

	public void checkEscape2A(int proposerId, boolean flag){
		if (flag && lastProposalId.get() == proposerId){
			escapeCheck.set(true);
		} else {
			escapeCheck.set(false);
		}
		lastProposalId.set(proposerId);
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
