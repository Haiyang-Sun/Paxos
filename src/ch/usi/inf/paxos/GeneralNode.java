package ch.usi.inf.paxos;

import java.util.Map.Entry;

import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.roles.Proposer;

public abstract class GeneralNode {
	int id;
	NetworkGroup networkGroup;
	NodeType nodeType;
	
	public NodeType getNodeType() {
		return nodeType;
	}

	public static enum NodeType {CLIENT, PROPOSER, ACCEPTOR, LEARNER};
	
	public int getId() {
		return id;
	}
	public NetworkGroup getNetworkGroup() {
		return networkGroup;
	}
	public GeneralNode(int id, NetworkGroup networkGroup) {
		super();
		this.id = id;
		this.networkGroup = networkGroup;
	}
	
	public void dispatchEvent(PaxosMessage msg){
	}
	
	public void eventLoop(){
	}
	
	public void backgroundLoop(){
	}
	
	@Override
	public String toString(){
		return "("+networkGroup.getGroupName()+","+networkGroup.getPort()+","+id+")";
	}
	public void onTimeout(PaxosMessage paxosMessage) {
		
	}
	
	public boolean hasNextEvent(){
		return false;
	}
	public PaxosMessage nextEvent(){
		return null;
	}
	
	public static class DispatchThread implements Runnable{
		GeneralNode node;
		public DispatchThread(GeneralNode proposer) {
			super();
			this.node = proposer;
		}
		@Override
		public void run() {
			while(true){
				while(!node.hasNextEvent()) {
					try {
						Thread.sleep(PaxosConfig.fetchEventInterval);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				node.dispatchEvent(node.nextEvent());
			}
		}
	}
}
