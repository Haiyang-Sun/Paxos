package ch.usi.inf.paxos;

import java.util.Map.Entry;

import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.messages.PaxosMessage;

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
}
