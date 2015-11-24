package ch.usi.inf.paxos;

import ch.usi.inf.logging.Logger;
import ch.usi.inf.paxos.roles.Acceptor;
import ch.usi.inf.paxos.roles.Client;
import ch.usi.inf.paxos.roles.Learner;
import ch.usi.inf.paxos.roles.Proposer;

public class Main {
	
	/*
	 * args
	 * 0 - type client | proposer | acceptor | learner
	 * 1 - id
	 * 2 - path to config
	 */
	public static void main(String args[]){
		String type = args[0];
		int id = Integer.parseInt(args[1]);
		if(!PaxosConfig.initFromFile(args[2])){
			Logger.error("Cannot parse the configuration file");
			return;
		}
		GeneralNode node = null;
		switch(type){
			case "client":
				node = Client.getById(id);
				break;
			case "proposer":
				node = Proposer.getById(id, PaxosConfig.extraThreadDispatching);
				break;
			case "acceptor":
				node = Acceptor.getById(id, PaxosConfig.extraThreadDispatching);
				break;
			case "learner":
				node = Learner.getById(id);
				break;
		}
		if(node != null) {
			new Thread(new BackgroundThread(node)).start();
			node.eventLoop();
		} else {
			Logger.error("Wrong node type specified");
		}
	}
	
	static class BackgroundThread implements Runnable{
		GeneralNode node;
		public BackgroundThread(GeneralNode node) {
			super();
			this.node = node;
		}
		@Override
		public void run() {
			node.backgroundLoop();
		}
		
	}
}
