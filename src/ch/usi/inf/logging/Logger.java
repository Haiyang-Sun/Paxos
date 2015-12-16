package ch.usi.inf.logging;

import ch.usi.inf.paxos.PaxosConfig;

public class Logger {
    public enum LoggerType {DEBUG, MSG_DEBUG, INFO, ERROR, HEARTBEAT, MSG_SUBMIT};

    public static void debug(final String log){
        write(LoggerType.DEBUG,log);
    }
    public static void msgDebug(final String log){
        write(LoggerType.MSG_DEBUG,log);
    }
    public static void submitDebug(final String log){
        write(LoggerType.MSG_SUBMIT,log);
    }
    public static void info(final String log){
        write(LoggerType.INFO, log);
    }
    public static void error(final String log){
        write(LoggerType.ERROR, log);
    }
    public static void heartbeat(final String log){
        write(LoggerType.HEARTBEAT, log);
    }

    public static void write(final LoggerType type, final String log){
        switch(type){
        case DEBUG:
        	if(PaxosConfig.debug)
        		System.out.println("DEBUG: "+log);
            break;
        case MSG_DEBUG:
        	if(PaxosConfig.msgDebug)
        		System.out.println("MSG_DEBUG: "+log);
            break;
        case MSG_SUBMIT:
        	if(PaxosConfig.submitDebug)
        		System.out.println("MSG_SUBMIT: "+log);
            break;
        case INFO:
        	System.out.println(log);
            break;
        case HEARTBEAT:
        	if(PaxosConfig.msgHeartBeat)
        		System.out.println("MSG_HEARTBEAT: "+log);
            break;
        case ERROR:
            System.err.println("ERROR: "+log);
            break;
        }
    }
}
