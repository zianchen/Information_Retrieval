import java.io.PrintStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.InputStream;
public class javaSSH{
	public static void main(String[] args) throws Exception{
	ProcessBuilder pb = new ProcessBuilder("ssh", 
                                       "node1601", 
                                       " $HADOOP_HOME/bin/hadoop jar /home/wei6/wei6Project3/probabilitySearch/booleanRetrieval.jar Driver");
pb.redirectErrorStream(); //redirect stderr to stdout
Process process = pb.start();
//InputStream inputStream = process.getInputStream();
//BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//String line = null;
//while((line = reader.readLine())!= null) {
   // System.out.println(line);
//}
//process.waitFor();
	}
}
