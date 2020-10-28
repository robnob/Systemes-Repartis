package CalculDistribue;

//import java.util.concurrent.TimeUnit;
//import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;


public class Clean {

	public static void main(String[] args) {
		String machine;
		Process p;
		try {
			BufferedReader br;
			br = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));
			while ((machine = br.readLine()) != null) {
				System.out.println(machine);
			    machine="rnobrega@"+machine;
			    String[] commande = { "ssh", "-o StrictHostKeyChecking=no", machine, "hostname" };
			    ProcessBuilder pb = new ProcessBuilder(commande);
		        pb.redirectErrorStream(true);
		        p = pb.start();
		        BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
		        String name=br2.readLine();
		        if (machine.equals("rnobrega@"+name)) {
		        	String[] delete_dir_cmd = {"ssh", "-o StrictHostKeyChecking=no", machine, "rm", "-rf", "/tmp/rnobrega/"};
		        	ProcessBuilder pb_clean = new ProcessBuilder(delete_dir_cmd);
			        p = pb_clean.start();
			        //p.waitFor();
		        }
		        br2.close();
			}
			br.close();
		} catch (IOException e) {
            e.printStackTrace();
        }
	}
}
