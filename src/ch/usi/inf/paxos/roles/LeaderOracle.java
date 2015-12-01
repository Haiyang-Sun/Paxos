package ch.usi.inf.paxos.roles;

import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.leader.PaxosAskForLeaderMessage;
import ch.usi.inf.paxos.messages.leader.PaxosNewLeaderMessage;
import ch.usi.inf.paxos.messages.leader.PaxosRunForLeaderMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosDecisionMessage;
import ch.usi.inf.paxos.messages.proposer.PaxosPhase1AMessage;

public class LeaderOracle {

	public static class Leader{
		public Leader(Proposer node, int generation) {
			this.leaderNode = node;
			this.generation = generation;
		}
		Proposer leaderNode = null;
		public Proposer getLeaderNode() {
			return leaderNode;
		}
		public int getGeneration() {
			return generation;
		}
		int generation = -1;
	}
	
	LeaderOracle(Proposer self){
		this.self = self;
	}
	
	Proposer self;
	public Proposer getSelf() {
		return self;
	}

	Leader leader = null;
	public Leader getLeader() {
		return leader;
	}

	public Proposer getLeaderNode(){
		return leader==null?null:leader.getLeaderNode();
	}
	
	public void runForLeader(){
		//PaxosMessenger.send(PaxosConfig.getAcceptorNetwork(), new PaxosRunForLeaderMessage(self));
	}
	
	public boolean selfIsLeader(){
		/* 
		 * return true would be the trivial solution
		 */
		return true;
		//return leader!=null && leader.getLeaderNode().getId()==self.getId();
	}
	
	public void loop(){
		if(selfIsLeader()){
			/*
			 * I'm the leader
			 * Send heart beat to all acceptors 
			 */
		}
	}
	
	public void onAskForLeader(PaxosAskForLeaderMessage msg){
		runForLeader();
	}
	
	public synchronized void onReceiveLeaderInfo(PaxosNewLeaderMessage msg){
		Leader receivedLeader = msg.getLeader();
		if(receivedLeader.generation < leader.generation){
			//ignore
		}else{
			leader = receivedLeader;
		}
	}
}
