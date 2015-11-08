package ch.usi.inf.testcases.network;

import java.nio.ByteBuffer;
import java.util.Arrays;

import ch.usi.inf.network.BaseMulticast;

public class BroadCastTest {
	static String group  = "228.5.6.7";//"192.168.192.60";
	
	static BaseMulticast g1 = new BaseMulticast(6103, group);
	static BaseMulticast g2 = new BaseMulticast(6103, group);
	
	public static void main(String[] args) {
		new Thread(new MulticastServerThread()).start();
		while(true) {
			ByteBuffer buf = g2.receive(1024);
			byte[] data = new byte[buf.position()];
			System.out.println(data.length);
			System.out.println(Arrays.toString(buf.array()));
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public static class MulticastServerThread implements Runnable{
		@Override
		public void run() {
			int total = 1024;
			int size = 32;
			int cnt = 0;
			byte[] buf = new byte[size];
			for(int i = 0; i < size; i++)
				buf[i] = (byte)(i%10);
			while(true){
				try {
					g1.send(buf, 0, buf.length);
					Thread.sleep(600);
					cnt+=size;
					if(cnt >= total)
						break;
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
}
