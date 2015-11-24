package ch.usi.inf.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;

public class BaseMulticast extends Multicast{
	NetworkGroup target;
	InetAddress group = null;
	MulticastSocket socket = null;
	public BaseMulticast(int port, String groupName){
		this.target = new NetworkGroup(groupName, port);
		init();
	}
	public BaseMulticast(NetworkGroup target){
		this.target = target;
		init();
	}
	
	void init(){
		try {
			socket = new MulticastSocket(target.port);
			group = InetAddress.getByName(target.groupName);
			socket.joinGroup(group);
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	@Override
	public void send(byte[] buf, int start, int length){
		DatagramPacket packet = new DatagramPacket(buf, start, length, group, target.port);
        try {
			socket.send(packet);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}
	
	@Override
	public ByteBuffer receive(int maxLength){
			byte[] buf = new byte[maxLength];
			DatagramPacket packet = new DatagramPacket(buf, maxLength);
			try {
				socket.receive(packet);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
			ByteBuffer res = ByteBuffer.allocate(maxLength);
			res.put(packet.getData(), 0, packet.getLength());
			return res;
	}
}
