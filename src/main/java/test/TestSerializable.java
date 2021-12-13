package test;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import vacuum.VacuumFilter;

public class TestSerializable {

	public static void main(String[] args) throws IOException, ClassNotFoundException {

		FileOutputStream fos = new FileOutputStream("C:/PHAMCHITAM/Document/Project/testProject/project");
		ObjectOutputStream oos = new ObjectOutputStream(fos);


		String tmp = "entryNum00327774524";
		int maxItems = 16;
		int slots = 4;
		int max_kick_steps = 50;
		
		VacuumFilter vf = new VacuumFilter(16);
		vf.init(maxItems, slots, max_kick_steps);
		vf.insert(tmp);
		vf.insert(tmp);
		vf.insert(tmp);
		vf.insert(tmp);
		vf.insert(tmp);
		vf.insert(tmp);
		vf.insert(tmp);
		vf.insert(tmp);
		vf.insert(tmp);
		vf.insert(tmp);
		System.out.println("vf lookup: " + vf.lookup(tmp));
		
		oos.writeObject(vf);
		
		FileInputStream fis = new FileInputStream("C:/PHAMCHITAM/Document/Project/testProject/project");
		ObjectInputStream ois = new ObjectInputStream(fis);

		
		VacuumFilter vf_recover = (VacuumFilter) ois.readObject();
		System.out.println("vf_recover lookup: " + vf_recover.lookup(tmp));
		
		

	}

}
