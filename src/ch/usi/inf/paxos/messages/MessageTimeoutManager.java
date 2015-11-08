package ch.usi.inf.paxos.messages;

import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import ch.usi.inf.paxos.GeneralNode;
import ch.usi.inf.paxos.PaxosConfig;
import ch.usi.inf.paxos.messages.client.PaxosClientMessage;

public class MessageTimeoutManager {
	ConcurrentHashMap<PaxosMessage, Long> timeoutRecords = new ConcurrentHashMap<PaxosMessage, Long>();
	
	GeneralNode node;
	
	
	public MessageTimeoutManager(GeneralNode node) {
		super();
		this.node = node;
	}

	public void check(){
		Set<Entry<PaxosMessage, Long>> records = timeoutRecords.entrySet();
		for(Entry<PaxosMessage, Long> record : records){
			long now = System.nanoTime();
			if(now - record.getValue() > 1000000.0 * PaxosConfig.timeoutMilisecs)
			{
				remove(record.getKey());
				node.onTimeout(record.getKey());
			}
		}
	}
	public void add(PaxosMessage msg) {
		timeoutRecords.putIfAbsent(msg, System.nanoTime());
	}
	public void remove(PaxosMessage msg) {
		timeoutRecords.remove(msg);
	}
}
