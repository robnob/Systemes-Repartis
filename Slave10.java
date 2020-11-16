package CalculDistribue;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
/*import java.util.List;
import java.nio.file.Path;
import java.util.Scanner;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;*/



public class Slave10 {

	public static void main(String[] args) throws IOException, InterruptedException {
		//HashMap<String, Integer> H = new HashMap<>();
		String line;
		if (args.length == 2) {
			//Création des dossiers "maps"
			ProcessBuilder pb_mkdir_maps = new ProcessBuilder("mkdir", "-p", "/tmp/rnobrega/maps/");
			Process p1 = pb_mkdir_maps.start();
			p1.waitFor();
			
			//Creation des fichiers de Mapping à partir des Splits
			PrintWriter writerM = new PrintWriter("/tmp/rnobrega/maps/UM"+args[0]+".txt", "UTF-8");
			BufferedReader br = new BufferedReader(new FileReader(args[1]));
			while ((line = br.readLine()) != null) {
				for(String word: line.split(" ")) {
					writerM.println(word+" 1");
					//H.put(word, H.getOrDefault(word,0)+1);
				}
			}
			br.close();
			writerM.close();
		}
		
		if (args.length == 4){
			//Creation des dossiers "shuffles"
			ProcessBuilder pb_mkdir_shuff = new ProcessBuilder("mkdir", "-p", "/tmp/rnobrega/shuffles/");
			Process p2 = pb_mkdir_shuff.start();
			p2.waitFor();
			
			//Création des fichiers dans Shuffle: Un fichier par mot en utilisant le HashCode pour le nom du fichier + "-" le nom de la machine
			BufferedReader brM = new BufferedReader(new FileReader(args[3]));
			int codigo;
			while ((line = brM.readLine()) != null) {
				String cle = line.split(" ")[0];
				codigo = cle.hashCode();
				FileWriter archivo = new FileWriter("/tmp/rnobrega/shuffles/"+codigo+"-"+args[2]+".txt", true);
				PrintWriter writerH = new PrintWriter(archivo);
				writerH.println(line);
				writerH.close();
			}
			brM.close();
			
			//Creation des dossiers "shufflesreceived"
			ProcessBuilder pb_mkdir_shrec = new ProcessBuilder("mkdir", "-p", "/tmp/rnobrega/shufflesreceived/");
			Process p3 = pb_mkdir_shrec.start();
			p3.waitFor();
			
		}	
		
		if (args.length == 0) {	
			//Creation des dossiers "reduces"
			ProcessBuilder pb_mkdir_reduces = new ProcessBuilder("mkdir", "-p", "/tmp/rnobrega/reduces/");
			Process p = pb_mkdir_reduces.start();
			p.waitFor();
			
			//Copie des fichiers shuffles dans shufflesreceived
			String host = InetAddress.getLocalHost().getHostName();
			File shuffles_content = new File("/tmp/rnobrega/shuffles/");
			String list_sh_cont[] = shuffles_content.list();
			for (int i=0; i < list_sh_cont.length; i ++) {
				String nom_fichier =list_sh_cont[i];
				int hshcode = Integer.parseInt(nom_fichier.split("-")[0]);
				int choix = (hshcode % 3);
				String mach_cible = Files.readAllLines(Paths.get("/tmp/rnobrega/machines.txt")).get(choix);
				String[] copy_sh = {"scp", "-o StrictHostKeyChecking=no", "-r", "-p", host+":/tmp/rnobrega/shuffles/"+nom_fichier,mach_cible+":/tmp/rnobrega/shufflesreceived/"};
				ProcessBuilder pb_copy_sh = new ProcessBuilder(copy_sh);
				pb_copy_sh.redirectErrorStream(true);
				Process p6 = pb_copy_sh.start();
				p6.waitFor();
			}
			
		}
		
		if (args.length == 1) {	
			//Regroupement des fichiers avec le même hash.
			String lineR;
			File shrec_cont = new File("/tmp/rnobrega/shufflesreceived/");
			String list_shrec_cont[] = shrec_cont.list();
			HashMap<String, Integer> simple = new HashMap<>();
			for (int i=0; i < list_shrec_cont.length; i ++) {
				String nom_fichier =list_shrec_cont[i];
				@SuppressWarnings("resource")
				BufferedReader brRec = new BufferedReader(new FileReader("/tmp/rnobrega/shufflesreceived/"+nom_fichier));
				while ((lineR = brRec.readLine()) != null) {
					String cle = lineR.split(" ")[0];
					simple.put(cle , (simple.getOrDefault(cle,0)+1));
				}
			}
			//Création du fichier avec le résultat de "Reduce"
			for (Map.Entry<String, Integer> couple : simple.entrySet()){
				String llave = couple.getKey();
				int codigo = llave.hashCode();
				FileWriter archivo = new FileWriter("/tmp/rnobrega/reduces/"+codigo+".txt", true);
				PrintWriter writerS = new PrintWriter(archivo);
				writerS.println(llave + " " + couple.getValue() );
				writerS.close();
			}
		}
	}
}

