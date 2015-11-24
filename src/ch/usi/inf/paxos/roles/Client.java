package ch.usi.inf.paxos.roles;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.GeneralNode.NodeType;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.client.PaxosClientMessage;

public class Client extends GeneralNode{
	static ConcurrentHashMap<Integer, Client> instances = new ConcurrentHashMap<Integer, Client>();
	ArrayList<ValueType> inputValues = new ArrayList<ValueType>();
	Client(int id, NetworkGroup networkGroup) {
		super(id, networkGroup);
	}
	
	/* 
	 * send every value read from stdandard input to proposers
	 * there is no guarantee that the value proposed will be received by any proposer
	 */
	public void propose(ValueType value, int slotIdx){
		PaxosMessenger.send(PaxosConfig.getProposerNetwork(), new PaxosClientMessage(this, value, slotIdx));
	}
	
	/*
	 * in case the client messages got lost, we broadcast all input values periodically
	 * proposers will ignore these messages if they have been already received 
	 */
	@Override
	public void backgroundLoop() {
		while(true) {
			for(int i = 0; i < inputValues.size(); i++){
				propose(inputValues.get(i),i);
			}
			try {
				Thread.sleep(PaxosConfig.clientBroadCastTime);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public void eventLoop(){
		Scanner scanner = new Scanner(System.in);
		String line;
		int cnt = 0;
		while(scanner.hasNext()){
			line = scanner.nextLine();
			ValueType val = new ValueType(line);
			inputValues.add(val);
			propose(val, cnt);
			cnt++;
		}
	}
	
	public static Client getById(int id){
		Client tmp = new Client(id, PaxosConfig.getClientNetwork());
		Client res = instances.putIfAbsent(id, tmp);
		if(res == null)
			return tmp;
		else
			return res;
	}
	
	@Override
	public NodeType getNodeType() {
		return NodeType.CLIENT;
	}
}
