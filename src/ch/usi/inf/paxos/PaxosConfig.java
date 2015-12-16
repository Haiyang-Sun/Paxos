package ch.usi.inf.paxos;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

import ch.usi.inf.logging.Logger;
import ch.usi.inf.network.NetworkGroup;
import ch.usi.inf.paxos.roles.Proposer;

public class PaxosConfig {
	public static boolean debug = false;
	public static boolean msgDebug = false;
	public static boolean msgHeartBeat = false;
	public static boolean submitDebug = false;
	public static boolean logClock = true;

	public static final int NUM_ACCEPTORS = 3; 
	public static final int NUM_QUORUM = 2; 

	public static boolean extraThreadDispatching = true;
	public static boolean escapePhase1 = false;
	
	static NetworkGroup clientNetwork;
	static NetworkGroup proposerNetwork;
	static NetworkGroup acceptorNetwork;
	static NetworkGroup learnerNetwork;
	public enum NetworkLevel {NORMAL};
	static NetworkLevel networkLevel = NetworkLevel.NORMAL;

	public static long decisionBroadcastIntervalMilisecs = 200;
	public static long timeoutMilisecs = 10;
	public static int randomSleep = 100;
	public static long clientBroadCastTime = 10;
	public static long fetchEventInterval = 10; //wait time when the event list is empty
	public static long timeoutCheckInterval = timeoutMilisecs / 2;
	public static long leaderHBInterval = 1000;
	public static long proposerInterval = leaderHBInterval;
	public static long susptIntervalCnt = 3;
	public static long learnerFetchInterval = 100;

	public static boolean initFromFile(String path){
		try {
			File file = new File(path);
			Scanner scanner = new Scanner(file);
			String line = null;
			try{
				while(scanner.hasNextLine()){
					line=scanner.nextLine();
					String[] elems = line.split("\\s+");
					switch (elems[0]){
						case "clients":
							PaxosConfig.clientNetwork = new NetworkGroup(elems[1], Integer.parseInt(elems[2]));
							break;
						case "proposers":
							PaxosConfig.proposerNetwork = new NetworkGroup(elems[1], Integer.parseInt(elems[2]));
							break;
						case "acceptors":
							PaxosConfig.acceptorNetwork = new NetworkGroup(elems[1], Integer.parseInt(elems[2]));
							break;
						case "learners":
							PaxosConfig.learnerNetwork = new NetworkGroup(elems[1], Integer.parseInt(elems[2]));
							break;
					}
				}
			}catch (Exception e){
				e.printStackTrace();
				Logger.error("Error input format");
				return false;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public static NetworkGroup getClientNetwork() {
		return clientNetwork;
	}
	public static NetworkGroup getProposerNetwork() {
		return proposerNetwork;
	}
	public static NetworkGroup getAcceptorNetwork() {
		return acceptorNetwork;
	}
	public static NetworkGroup getLearnerNetwork() {
		return learnerNetwork;
	}
	
	public static NetworkLevel getNetworkLevel() {
		return networkLevel;
	}
}
