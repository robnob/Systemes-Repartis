package CalculDistribue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Deployer10 implements Runnable {
	private String machine;
	private String number;
	private String mode;
    
    public Deployer10(String m, String n, String mode){
        this.machine=m;
        this.number=n;
        this.mode = mode;
    }
    @Override
    public void run() {
    	ProcessBuilder pb = new ProcessBuilder("ssh", "-o StrictHostKeyChecking=no", machine, "hostname");
        pb.redirectErrorStream(true);
        try {
			if (mode == "MAP") {
				Process p = pb.start();
				BufferedReader br = new BufferedReader(new InputStreamReader((p.getInputStream())));
				String name=br.readLine();
				if (machine.equals("rnobrega@"+name)) {
					
					//Copie de "machines.txt" dans toutes les machines du pool
					String[] copy_mach_cmd = {"scp", "-o StrictHostKeyChecking=no", "-r", "-p", "/cal/homes/rnobrega/syst_rep/machines.txt",machine+":/tmp/rnobrega/"};
					ProcessBuilder pb_copy_mach = new ProcessBuilder(copy_mach_cmd);
					pb_copy_mach.redirectErrorStream(true);
					p = pb_copy_mach.start();
					p.waitFor();
					
					//Copie de "Slave10.jar" dans outes les machines du pool 
					String[] copy_slave_cmd = {"scp", "-o StrictHostKeyChecking=no", "-r", "-p", "/cal/homes/rnobrega/syst_rep/Slave10.jar",machine+":/tmp/rnobrega/"};
					ProcessBuilder pb_copy_slave = new ProcessBuilder(copy_slave_cmd);
					pb_copy_slave.redirectErrorStream(true);
					p = pb_copy_slave.start();
					p.waitFor();
					
					//Application du MAP (2 arguments pour le .jar)
					String[] launch_slave_cmd = {"ssh", "-o StrictHostKeyChecking=no", machine, "java","-jar", "/tmp/rnobrega/Slave10.jar", number,
							"/tmp/rnobrega/splits/S"+String.valueOf(number)+".txt"};
					ProcessBuilder pb_launch_slave = new ProcessBuilder(launch_slave_cmd);
					pb_launch_slave.redirectErrorStream(true);
					p = pb_launch_slave.start();
					p.waitFor();
				}
				br.close();
			}
			if (mode == "SHUFFLE") {
				Process p = pb.start();
				BufferedReader br2 = new BufferedReader(new InputStreamReader((p.getInputStream())));
				String name=br2.readLine();
				if (machine.equals("rnobrega@"+name)) {
					//Application de Shuffle avec 4 arguments pour le .jar
					String[] launch_slave_cmd = {"ssh", "-o StrictHostKeyChecking=no", machine, "java","-jar", "/tmp/rnobrega/Slave10.jar",
					number,
					"/tmp/rnobrega/splits/S"+String.valueOf(number)+".txt",
					name, 
					"/tmp/rnobrega/maps/UM"+String.valueOf(number)+".txt"};
					ProcessBuilder pb_launch_slave = new ProcessBuilder(launch_slave_cmd);
					pb_launch_slave.redirectErrorStream(true);
					p = pb_launch_slave.start();
					p.waitFor();
				}
				br2.close();
			}	
			if (mode == "PRE-REDUCE") {
				Process p = pb.start();
				BufferedReader brR = new BufferedReader(new InputStreamReader((p.getInputStream())));
				String nameR=brR.readLine();
				if (machine.equals("rnobrega@"+nameR)) {
					String[] launch_slave_red = {"ssh", "-o StrictHostKeyChecking=no", machine, "java","-jar", "/tmp/rnobrega/Slave10.jar"};
					ProcessBuilder pb_launch_reduce = new ProcessBuilder(launch_slave_red);
					pb_launch_reduce.redirectErrorStream(true);
					p = pb_launch_reduce.start();
					p.waitFor();
				}
				brR.close();
			}	
			
			if (mode == "REDUCE") {
				Process p = pb.start();
				BufferedReader brR = new BufferedReader(new InputStreamReader((p.getInputStream())));
				String nameR=brR.readLine();
				if (machine.equals("rnobrega@"+nameR)) {
					String[] launch_slave_red = {"ssh", "-o StrictHostKeyChecking=no", machine, "java","-jar", "/tmp/rnobrega/Slave10.jar", nameR};
					ProcessBuilder pb_launch_reduce = new ProcessBuilder(launch_slave_red);
					pb_launch_reduce.redirectErrorStream(true);
					p = pb_launch_reduce.start();
					p.waitFor();
				}
				brR.close();
			}			
        }catch (InterruptedException e) {
			e.printStackTrace();
		}catch (IOException e) {
			e.printStackTrace();
		} 
    }
    @Override
    public String toString(){
        return this.machine;
    }
}


