package quotient;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import hash.MurmurHash_QF;

public class Test {

	public static void main(String[] args) throws IOException {
		FileInputStream fis = new FileInputStream("C:/PHAMCHITAM/Document/Project/SelfJoinData/Data.txt");

		
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		
		String line = br.readLine();
		
		int count = 0;
		long hash = 0;

		
//		while((line != null)&& (count < 4000000)) {
		
		while(line != null) {
			hash = MurmurHash_QF.hash64(line);
			line = br.readLine();
			count++;

		}
	System.out.println(count + " hash: " + hash);
	
	}

}
