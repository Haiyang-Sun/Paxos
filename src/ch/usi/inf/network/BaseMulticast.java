package ch.usi.inf.network;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.nio.ByteBuffer;

public class BaseMulticast extends Multicast{
	NetworkGroup target;
	
	public BaseMulticast(int port, String groupName){
		this.target = new NetworkGroup(groupName, port);
	}
	public BaseMulticast(NetworkGroup target){
		this.target = target;
	}
	
	@Override
	public void send(byte[] buf, int start, int length){
		InetAddress group = null;
		MulticastSocket socket = null;
		try {
			socket = new MulticastSocket(target.port);
			group = InetAddress.getByName(target.groupName);
			socket.joinGroup(group);
			DatagramPacket packet = new DatagramPacket(buf, start, length, group, target.port);
	        socket.send(packet);
	        socket.leaveGroup(group);
		} catch (Exception e) {
			e.printStackTrace();
		} finally{
			if(socket != null)
				socket.close();
		}
	}
	
	@Override
	public ByteBuffer receive(int maxLength){
		MulticastSocket socket = null;
		try{
			socket = new MulticastSocket(target.port);
			InetAddress group = InetAddress.getByName(target.groupName);
			socket.joinGroup(group);
			byte[] buf = new byte[maxLength];
			DatagramPacket packet = new DatagramPacket(buf, maxLength);
			socket.receive(packet);
			socket.leaveGroup(group);
			ByteBuffer res = ByteBuffer.allocate(maxLength);
			res.put(packet.getData(), 0, packet.getLength());
			return res;
		}catch(IOException e){
			e.printStackTrace();
			return null;
		}finally{
			if(socket != null)
				socket.close();
		}
	}
}
