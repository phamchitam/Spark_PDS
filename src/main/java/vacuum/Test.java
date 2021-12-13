package vacuum;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;

public class Test {

	public static void main(String[] args) throws IOException {
		
		long begin = System.currentTimeMillis();
		int maxItems = 15000000;
		int slots = 4;
		int max_kick_steps = 1000;
		VacuumFilter vf = new VacuumFilter(16);
		vf.init(maxItems, slots, max_kick_steps);
		
		
		FileInputStream fis = new FileInputStream("C:/PHAMCHITAM/Document/Project/SelfJoinData/Data.txt");
		InputStreamReader isr = new InputStreamReader(fis);
		BufferedReader br = new BufferedReader(isr);
		
		String tmp = "entryNum15681705450";
		

		
		String line = br.readLine();
		int count = 0;
		
		while((line != null) && (count < 95000000))  {
			
				count = count + 1;
				String[] arr = line.split(",");
				
				vf.insert(arr[0]);
				if (vf.lookup(arr[0]) == false) {
					System.out.println("Error at " + count + " value: " + arr[0]);
					break;
				}
				
				line = br.readLine();


			}	


//			vf.insert(tmp);
//			System.out.println("Result1: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result2: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result3: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result4: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result5: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result6: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result7: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result8: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result9: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result10: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result11: " + vf.lookup(tmp));
//			vf.insert(tmp);
//			System.out.println("Result12: " + vf.lookup(tmp));
		
		long end = System.currentTimeMillis();
		
		long total = end - begin;
		
		System.out.println("Count: " + count + " Time " + total);
			
	}
	


}
