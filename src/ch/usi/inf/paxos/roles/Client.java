package ch.usi.inf.paxos.roles;

import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;

import ch.usi.inf.logging.Logger;
import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.ValueType;
import ch.usi.inf.paxos.messages.PaxosMessage;
import ch.usi.inf.paxos.messages.PaxosMessenger;
import ch.usi.inf.paxos.messages.client.PaxosClientMessage;
import ch.usi.inf.paxos.messages.leader.PaxosClientSuccessMessage;

public class Client extends GeneralNode{
	static ConcurrentHashMap<Integer, Client> instances = new ConcurrentHashMap<Integer, Client>();
	
	private static int offset = 0;
	CopyOnWriteArrayList<ValueType> inputValues = new CopyOnWriteArrayList<ValueType>();
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
		if(PaxosConfig.extraThreadDispatching)
			new Thread(new DispatchThread(this)).start();
		while(true) {
			if (offset < inputValues.size())
				propose(inputValues.get(offset), offset);
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
		new Thread(new ScannerThread(this)).start();
		while(true){
			PaxosMessage msg = PaxosMessenger.recv(this.getNetworkGroup());
			dispatchEvent(msg);
		}
	}
	
	public static class ScannerThread implements Runnable{
		private Client client;
		public ScannerThread(Client client){
			this.client = client;
		}

		@Override
		public void run() {
			Scanner scanner = new Scanner(System.in);
			String line;
			while(scanner.hasNext()){
				line = scanner.nextLine();
				ValueType val = new ValueType(line);
				client.inputValues.add(val);
			}
		}
	}

	@Override
	public void dispatchEvent(PaxosMessage msg){
			switch (msg.getType()){
				case MSG_PROPOSER_CLIENT_SUCCESS:
					onClientSuccess(msg);
					break;
			}
	}
	
	private void onClientSuccess(PaxosMessage msg) {
		PaxosClientSuccessMessage successMsg = (PaxosClientSuccessMessage) msg;
		
		//message is for other client
		if (successMsg.getClientId() != id){
			return;
		}

		int slotIndex = successMsg.getSlotIndex();
		if (slotIndex == offset) {
			Logger.submitDebug("received comfirm of local slot: " + slotIndex);
			offset++;
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
