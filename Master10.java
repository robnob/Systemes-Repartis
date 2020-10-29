package CalculDistribue;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
//import java.util.concurrent.TimeUnit;

//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ThreadPoolExecutor;
//import java.util.concurrent.Future;
//import java.util.concurrent.TimeUnit;
//import java.io.FileNotFoundException;
//import java.io.InputStreamReader;

public class Master10 {

	public static void main(String[] args) throws InterruptedException {
		String machine;
		int Sleeping_time = 5000;
		//Process p;
		try {
			//SPLITTING THE FILE	
			System.out.println("SPLITTING !!!");
			int nb_machines = 0;
			BufferedReader br = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));
			while (br.readLine() != null) {
				nb_machines++;
			}
			br.close();
			int nb_lignes = 0;
			BufferedReader brl = new BufferedReader(new FileReader("/tmp/rnobrega/input.txt"));
			while (brl.readLine() != null) {
				nb_lignes++;
			}
			brl.close();
			int q = (nb_lignes+nb_machines-1) / nb_machines;
			for (int m = 0; m <=nb_machines-1; m++) {
				FileWriter archivo = new FileWriter("/tmp/rnobrega/splits/S"+m+".txt", true);
				PrintWriter writerS = new PrintWriter(archivo);
				int end= (m+1)*q-1;
				if (end > nb_lignes -1) {
					end = nb_lignes -1;
				}
				for (int i= m*q; i <=end; i++) {
					if (i > nb_lignes) {
						break;
					}else {
						System.out.println(i);
						writerS.println(Files.readAllLines(Paths.get("/tmp/rnobrega/input.txt"), StandardCharsets.UTF_8).get(i));
					}
				}
				writerS.close();
			}
			ExecutorService executor = Executors.newFixedThreadPool(4);
			//ExecutorService executor = new ThreadPoolExecutor(4,4, 1000, TimeUnit.SECONDS );
			int count=0;
			long tempsM = 0;
			System.out.println("MAPPING !!!");
			
			//MAPPING			
			BufferedReader brM = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));
			while ((machine = brM.readLine()) != null) {
			    //System.out.println("Mapping   "+machine);
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
				
				//MAPPING
				long startM = System.nanoTime();
			    Runnable worker = new Deployer10(machine,String.valueOf(count), "MAP");
	            executor.execute(worker);
	            Thread.sleep(Sleeping_time);
	            count++;    
	            long endM = System.nanoTime();
	        	tempsM = tempsM + endM - startM;
	        }
			brM.close();
			System.out.println("MAP terminé");
			System.out.println("Mapping time: "+ (tempsM-0)/1000000);	
	        	
	        //SHUFFLING
	        BufferedReader br2;
			br2 = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));
			int countShuf=0;
			long tempsS =0;
			System.out.println("SHUFFLING !!!");
			while ((machine = br2.readLine()) != null) {
			    //System.out.println("shuffling   "+machine);
			    machine="rnobrega@"+machine;
			    long startS = System.nanoTime();
			    Runnable worker = new Deployer10(machine,String.valueOf(countShuf), "SHUFFLE");
	            executor.execute(worker);
	            Thread.sleep(Sleeping_time);
		        countShuf++;  
		        long endS = System.nanoTime();
	        	tempsS = tempsS + endS - startS;
	        	
			}
			br2.close();
			System.out.println("SHUFFLE terminé");
			System.out.println("Shuffling time: "+ (tempsS-0)/1000000);
			
	      //PRE-REDUCING
	        BufferedReader brPR = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));;
			int countPRed=0;
			long tempsP = 0;
			System.out.println("PREREDUCING !!!");
			while ((machine = brPR.readLine()) != null) {
			    //System.out.println("Pre-reducing   "+machine);
			    machine="rnobrega@"+machine;
			    long startP = System.nanoTime();
			    Runnable worker = new Deployer10(machine,String.valueOf(countPRed), "PRE-REDUCE");
	            executor.execute(worker);
	            Thread.sleep(Sleeping_time);
		        countPRed++;  
		        long endP = System.nanoTime();
	        	tempsP = tempsP + endP - startP;
	        	
			}	
			brPR.close();
			System.out.println("PRE-REDUCE terminé");
			System.out.println("Pre-Reducing time: "+ (tempsP-0)/1000000);
			
	      //REDUCING
	        BufferedReader brR;
			brR = new BufferedReader(new FileReader("/tmp/rnobrega/machines.txt"));
			int countRed=0;
			long tempsR = 0;
			System.out.println("REDUCING !!!");
			while ((machine = brR.readLine()) != null) {
			    //System.out.println("Reducing   "+machine);
			    machine="rnobrega@"+machine;
			    long startR = System.nanoTime();
			    Runnable worker = new Deployer10(machine,String.valueOf(countRed), "REDUCE");
	            executor.execute(worker);
	            Thread.sleep(Sleeping_time);
		        countRed++;
		        long endR = System.nanoTime();
	        	tempsR = tempsR + endR - startR;
	        }
			brR.close();
			
	        System.out.println("REDUCE terminé");
	        System.out.println("Reducing  time: "+ (tempsR-0)/1000000);
	        System.out.println("Pre + Reducing  time: "+ (tempsP+ tempsR -6000)/1000000);
	        Thread.sleep(5000);
	        executor.shutdown();
	        executor.shutdownNow();
	        System.out.println("Program terminé");
	        
		}catch (IOException e) {
            e.printStackTrace();
        }catch (InterruptedException i) {
        	i.printStackTrace();
        }
	}
}
