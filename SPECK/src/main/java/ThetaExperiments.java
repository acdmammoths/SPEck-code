import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;

public class ThetaExperiments {
    // Used to determine values of theta for experiments
    public static void getNumFSPs() throws IOException {
        for (double theta = .1; theta > .03; theta -= .005) {
            String fileIn = "./data/BIBLE.txt";
            String fileFSP = "fspResults.txt";

            AlgoPrefixSpan alg = new AlgoPrefixSpan();
            alg.runAlgorithm(fileIn,theta,fileFSP);

            FileReader fr = new FileReader(fileFSP);
            BufferedReader br = new BufferedReader(fr);

            int numLines = 0;
            while (br.readLine() != null) numLines++;

            File f = new File(fileFSP);
            f.delete();
            System.out.println(theta + " " + numLines);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String[] strategies = new String[]{"completePerm", "sameSizePerm"};
        String dataset = "BIBLE";
        String fileIn = "./data/BIBLE.txt";
        int P = 100;
        int T = 10000;
        int parallelization = 40;
        double fwer = 0.05;
        File resultsFile = new File("./results/thetaExperiments.csv");
        FileWriter fw = new FileWriter(resultsFile);
        fw.write("strategy,dataset,iter,theta,time,sfsp,fsp\n");
        for (int i = 0; i < 8; i++) {
            System.out.println("\t"+i);
	        for (String strategy : strategies) {
                for (double theta : new double[]{.031, .043, .06, .1}) {
            	    System.out.println(theta);
                    String fileOut = "./thetaExpResults_" + String.valueOf(theta).replace(".", "") + ".txt";
                    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SPEck").set("spark.executor.memory", "5g").set("spark.driver.memory", "5g").set("spark.executor.heartbeatInterval", "10000000").set("spark.network.timeout", "10000000");
                    JavaSparkContext scc = new JavaSparkContext(sparkConf);
                    PropertyConfigurator.configure("log4j.properties");
                    SPEck speck = new SPEck(fileIn, fileOut, scc, strategy);
                    long start = System.currentTimeMillis();
                    speck.execute(P, T, parallelization, theta, fwer);
                    scc.stop();
                    long timeElapsed = System.currentTimeMillis() - start;
                    int sfsp = speck.numSFSP;
                    int fsp = speck.numFSP;
                    fw.write(strategy +","+dataset+","+i+","+theta+","+timeElapsed+","+sfsp+","+fsp+"\n");
                    fw.flush();
               }
            }
	    }
        fw.close();
    }
}
