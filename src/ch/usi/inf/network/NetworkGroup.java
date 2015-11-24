package ch.usi.inf.network;

public class NetworkGroup {
	String groupName;
	int port;
	public NetworkGroup(String groupName, int port) {
		super();
		this.groupName = groupName;
		this.port = port;
	}
	public String getGroupName() {
		return groupName;
	}
	public void setGroupName(String groupName) {
		this.groupName = groupName;
	}
	public int getPort() {
		return port;
	}
	public void setPort(int port) {
		this.port = port;
	}
	@Override
	public int hashCode() {
		return groupName.hashCode()+port;
	}
	@Override
	public String toString(){
		return "("+groupName+","+port+")";
	}
}
