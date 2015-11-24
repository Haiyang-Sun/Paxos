package ch.usi.inf.logging;

import ch.usi.inf.paxos.PaxosConfig;

public class Logger {
    public enum LoggerType {DEBUG, INFO, ERROR};

    public static void debug(final String log){
        write(LoggerType.DEBUG,log);
    }
    public static void info(final String log){
        write(LoggerType.INFO, log);
    }
    public static void error(final String log){
        write(LoggerType.ERROR, log);
    }
    public static void write(final LoggerType type, final String log){
        switch(type){
        case DEBUG:
        	if(PaxosConfig.debug)
        		System.out.println("DEBUG: "+log);
            break;
        case INFO:
        	System.out.println(log);
            break;
        case ERROR:
            System.err.println("ERROR: "+log);
            break;
        }
    }
}
