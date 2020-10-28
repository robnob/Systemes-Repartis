package CalculDistribue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
//import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.io.FileNotFoundException;
//import java.io.InputStreamReader;
//import java.util.concurrent.TimeUnit;

public class Master10 {

	public static void main(String[] args) throws InterruptedException {
		String machine;
		//Process p;
		try {
			//MAPPING
			BufferedReader br;
			br = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));
			ExecutorService executor = Executors.newFixedThreadPool(4);
			//ExecutorService executor = new ThreadPoolExecutor(4,4, 1000, TimeUnit.SECONDS );
			int count=0;
			while ((machine = br.readLine()) != null) {
			    System.out.println("Mapping   "+machine);
			    machine="rnobrega@"+machine;
			    
			    //Création des dossiers "splits"
			    String[] create_dir_splits = {"ssh", "-o StrictHostKeyChecking=no", machine, " mkdir -p /tmp/rnobrega/splits/"};
				ProcessBuilder pb_mkdir_splits = new ProcessBuilder(create_dir_splits);
				Process p = pb_mkdir_splits.start();
				p.waitFor();
				
				//Copie des fichiers splits
				String[] copy_data_cmd = {"scp", "-o StrictHostKeyChecking=no", "-r", "-p", "/tmp/rnobrega/splits/S"+String.valueOf(count)+".txt",machine+":/tmp/rnobrega/splits/"};
				ProcessBuilder pb_copy_data = new ProcessBuilder(copy_data_cmd);
				pb_copy_data.redirectErrorStream(true);
				p = pb_copy_data.start();
				p.waitFor();
				
			    Runnable worker = new Deployer10(machine,String.valueOf(count), "MAP");
	            executor.execute(worker);
	            Thread.sleep(10000);
	            count++;       
			}
			br.close();
			System.out.println("MAP terminé");
	        
	        //SHUFFLING
	        BufferedReader br2;
			br2 = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));
			int countShuf=0;
			while ((machine = br2.readLine()) != null) {
			    System.out.println("shuffling   "+machine);
			    machine="rnobrega@"+machine;
			    Runnable worker = new Deployer10(machine,String.valueOf(countShuf), "SHUFFLE");
	            executor.execute(worker);
	            Thread.sleep(10000);
		        countShuf = countShuf +1;  
			}
			br2.close();
			//executor.shutdown();
	        System.out.println("SHUFFLE terminé");
	        
	      //PRE-REDUCING
	        BufferedReader brPR = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));;
			int countPRed=0;
			while ((machine = brPR.readLine()) != null) {
			    System.out.println("Pre-reducing   "+machine);
			    machine="rnobrega@"+machine;
			    Runnable worker = new Deployer10(machine,String.valueOf(countPRed), "PRE-REDUCE");
	            executor.execute(worker);
	            Thread.sleep(10000);
		        countPRed = countPRed +1;  
			}
			brPR.close();
	        
	      //REDUCING
	        BufferedReader brR;
			brR = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));
			int countRed=0;
			while ((machine = brR.readLine()) != null) {
			    System.out.println("Reducing   "+machine);
			    machine="rnobrega@"+machine;
			    Runnable worker = new Deployer10(machine,String.valueOf(countRed), "REDUCE");
	            executor.execute(worker);
	            Thread.sleep(20000);
		        countRed = countRed +1;  
			}
			brR.close();
			
	        System.out.println("REDUCE terminé");
	        Thread.sleep(10000);
	        executor.shutdown();
	        executor.shutdownNow();
	        
		}catch (IOException e) {
            e.printStackTrace();
        }catch (InterruptedException i) {
        	i.printStackTrace();
        }
	}
}
