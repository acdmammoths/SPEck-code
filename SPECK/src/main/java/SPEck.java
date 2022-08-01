import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.json.JSONObject;

import org.apache.log4j.*;
import java.io.*;
import java.util.*;
import java.util.stream.IntStream;

/**
 * This class is a parallel implementation of the algorithm SPEck described in
 * the paper "SPEck: Mining Statistically-significant Sequential Patterns
 * Efficiently with Exact Sampling".
 * This code was used to test the performance and correctness of the strategy proposed
 * in the paper and the results are reported in the Experiments section
 * of the paper.
 * SPEck aims to extract the set of significant frequent sequential patterns (SFSP)
 * from a sequential transactional dataset, given a minimum frequency threshold theta,
 * while bounding the probability that one or more false positives are reported in output.
 * It employs the Westfall-Young method to correct for multiple hypothesis testing.
 */

public class SPEck implements Serializable {
    transient JavaSparkContext scc;
    // input and output files
    String fileIn;
    String fileOut;
    // data structures to store the datasets data
    Int2ObjectOpenHashMap<Utils.Pair> positionOrigin;
    Utils.Dataset datasetOrigin;
    Object2IntOpenHashMap<String> itemsetsOrigin;
    Utils.Dataset datasetRandom;
    String strategy;
    String[] datasetFilePaths;
    int numSfsp;
    int numFsp;
    double correctedThreshold;
    ObjectArrayList<Utils.PairT<String, Double>> sfsp = new ObjectArrayList<>();
    Object2IntOpenHashMap<String> fspSup = new Object2IntOpenHashMap<>();

    /**
     * Constructor
     * @param   fileIn  file of the input dataset to mine the SFSP
     * @param   fileOut file to store the SFSP
     * @param   scc     JavaSparkContext for the parallel execution
     */
    SPEck(String fileIn, String fileOut, JavaSparkContext scc, String strategy){
        this.fileIn = fileIn;
        this.fileOut = fileOut;
        this.scc = scc;
        this.strategy = strategy;
        positionOrigin = new Int2ObjectOpenHashMap<>();
        datasetOrigin = new Utils.Dataset();
        itemsetsOrigin = new Object2IntOpenHashMap<>();
    }

    /**
     * Computes the p-values in parallel with a Monte Carlo procedure. It returns the p-values computed
     * by this core.
     * @param   index           the index of the core used
     * @param   fileMined       the file that contains the FSP mined from the actual dataset
     * @param   T               the number of random datasets for the Monte Carlo estimate of p-values
     * @param   parallelization the number of cores of the machine
     *                          (fixed for reproducibility)
     * @return                  the p-values computed by this core
     */

    private static int[] parallelComputePValues(
            int index, String fileMined, int T,
            int parallelization, String[] fp
    ) throws IOException {
        // data structures to store the actual dataset and the FSP mined from it
        Object2IntOpenHashMap<String> spSupp = new Object2IntOpenHashMap<>();
        Object2IntOpenHashMap<String> spIndex = new Object2IntOpenHashMap<>();
        // reads the FSP mined from the actual dataset and stores them in the data structures
        int numSP = readOut(fileMined,spSupp,spIndex);
        int[] pValue = new int[numSP];
        // generates T/parallelization random datasets with this core
        int start = T * (index - 1) / parallelization;
        int end = T*index / parallelization;
        for(int j=start;j<end;j++){
           String fileMinned = fp[j];
            // updates the p-values with the FSP of the random dataset generated
            updatePValue(fileMinned,spSupp,pValue,spIndex);
        }
        return pValue;
    }

    /**
     * Reads the dataset from FileIn file and stores the dataset in the data structures provided in input.
     * @param   fileIn          the name of the file that contains the dataset to load
     * @param   positions       an hashmap where it assigns an index to each position of the dataset
     * @param   dataset         an arraylist where it stores the index of the itemsets in the transactions of the dataset.
     *                          Each element of the arraylist represents a transactions and it is an arraylist of
     *                          integers. The integers in such arraylist are the indexes of the itemsets in that transaction.
     */
    protected static void loadDataset(
            String fileIn, Int2ObjectOpenHashMap<Utils.Pair> positions,
            Utils.Dataset dataset) throws IOException {
        Object2IntOpenHashMap<String> itemsets = new Object2IntOpenHashMap<>();
        FileReader fr = new FileReader(fileIn);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        int i = 0;
        int itemset = 0;
        int position = 0;
        while(line!=null){
            ObjectArrayList<Utils.Itemset> transaction = new ObjectArrayList<>();
            String[] splitted = line.split(" -1 ");
            for(int j=0;j<splitted.length-1;j++){
                Utils.Itemset is;
                if(!itemsets.containsKey(splitted[j])){
                    is = new Utils.Itemset(splitted[j], itemset, i);
                    itemsets.put(splitted[j],itemset);
                    itemset++;
                }
                else is = new Utils.Itemset(splitted[j], itemsets.getInt(splitted[j]), i);
                positions.put(position++,new Utils.Pair(i,j));
                transaction.add(is);
            }
            dataset.add(transaction);
            line = br.readLine();
            i++;
        }
        br.close();
        fr.close();
    }

    /**
     * Mines the dataset in the fileIn file using theta as minimum
     * frequency threshold and stores in fileOut the FSP found.
     * This method uses the PrefixSpan implementation provided in the SPMF library.
     * @param   fileIn   the name of the file that contains the dataset to mine
     * @param   fileFSP  the name of the file where the method stores the sequential patterns found
     * @param   theta    the minimum frequency threshold used to mine the dataset
     */
    protected static void mining(String fileIn, String fileFSP, double theta) throws IOException{
        AlgoPrefixSpan alg = new AlgoPrefixSpan();
        alg.runAlgorithm(fileIn,theta,fileFSP);
    }

    /**
     * Reads the file that contains the FSP mined from a dataset and stores them in two hashmaps.
     * It returns the number of FSP read.
     * @param   fileMined   the name of the file to read that contains the FSP mined from a dataset
     * @param   spSupp      an hashmap to store the FSP and their supports. The hashmap has strings that represent
     *                      the sequential patterns as keys and integers that represent the supports of the sequential
     *                      patterns as values
     * @param   spIndex     an hashmap to assign to each sequential pattern an integer index. The hashmap has strings
     *                      that represent the sequential patterns as keys and integers that represent the indexes
     *                      assigned to the sequential patterns as values. This hashmap is used to speed up the
     *                      computation of the p-values
     * @return              the number of the FSP read from the file
     */
    private static int readOut(String fileMined, Object2IntOpenHashMap<String> spSupp, Object2IntOpenHashMap<String> spIndex) throws IOException {
        FileReader fr = new FileReader(fileMined);
        BufferedReader br = new BufferedReader(fr);
        String line = br.readLine();
        int index = 0;
        while(line!=null){
            String[] splitted = line.split(" #SUP: ");
            spSupp.put(splitted[0],Integer.parseInt(splitted[1]));
            spIndex.put(splitted[0],index++);
            line = br.readLine();
        }
        br.close();
        fr.close();
        return spIndex.size();
    }

    /**
     * Updates the data for the p-values computation after the mining of a new random dataset.
     * @param   file    the name of the file with the FSP mined from the random dataset
     * @param   spSupp  the hashmap that contains the supports of the FSP mined from the starting dataset
     * @param   pValue  the array containing the data for the p-values computation
     * @param   spIndex the hashmap that contains the indexes of the FSP mined from the starting dataset
     */
    private static void updatePValue(String file, Object2IntOpenHashMap<String> spSupp, int[] pValue, Object2IntOpenHashMap<String> spIndex) throws IOException{
        BufferedReader br = null;
        FileReader fr = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
        } catch (Exception e) {
            System.err.println(file);
            System.exit(1);
        }

        String line = br.readLine();
        while(line!=null){
            String[] splitted = line.split(" #SUP: ");
            if(spSupp.containsKey(splitted[0])){
                int suppO = spSupp.getInt(splitted[0]);
                int suppS = Integer.parseInt(splitted[1]);
                if(suppO <= suppS) pValue[spIndex.getInt(splitted[0])]++;
            }
            line = br.readLine();
        }
        br.close();
        fr.close();
    }

    /**
     * Writes a random dataset in the FileOut file.
     * @param   fileOut         the name of the file where it writes the dataset
     * @param   dataset         the hashmap that contains the indexes of the itemsets in the dataset (See loadDataset method)
     */
    protected static void writeDataset(String fileOut, Utils.Dataset dataset) throws IOException {
        FileWriter fw = new FileWriter(fileOut);
        BufferedWriter bw = new BufferedWriter(fw);
        for (ObjectArrayList<Utils.Itemset> transaction : dataset) {
            StringBuilder line = new StringBuilder();
            for (Utils.Itemset itemset : transaction) {
                line.append(itemset.getString()).append(" -1 ");
            }
            line.append("-2\n");
            bw.write(line.toString());
        }
        bw.close();
        fw.close();
    }

    /**
     * Compute the p-values of the FSP of a dataset merging the p-values computed by the different cores.
     * @param   T               the number of random datasets for the Monte Carlo estimate of p-values
     * @param   parallelization the number of cores of the machine. It is the number of random datasets
     *                          generated at the same time in the Monte Carlo estimation. Each core of
     *                          the machine computes the p-values on T/parallelization random datasets.
     * @param   fileMined       the file containing the FSP extracted from the dataset stored in file
     * @return
     */
    private int[] computePValues(int T, int parallelization, String fileMined){
        // the indexes of the cores of the machine
        IntArrayList indexes = new IntArrayList();
        for (int i = 0; i < parallelization; i++) indexes.add(i+1);
        // computes in parallel the p-values from different random datasets and merges the p-values computed by the different cores
        int[] pValueInt =  scc.parallelize(indexes, parallelization).map(o1 -> parallelComputePValues(
                o1, fileMined, T, parallelization, datasetFilePaths
        )).reduce((a, b) -> IntStream.range(0, a.length).map(i -> a[i] + b[i]).toArray());

        return pValueInt;
    }

    /**
     * Generates T datasets and mines them in parallel, storing the results on disk.
     * @param   t               the number of random datasets for the Monte Carlo estimate of p-values
     * @param   theta           the minimum frequency threshold used to mine the dataset
     */

    private void generateTDatasets(int t, double theta, int parallelization) {
        datasetFilePaths = new String[t];
        // the indexes of the cores of the machine
        IntArrayList indexes = new IntArrayList();
        for (int i = 0; i < parallelization; i++) indexes.add(i+1);

        List<String[]> results = scc.parallelize(indexes, parallelization).map(o1 -> parallelGenDataset(o1, t, theta,
                strategy, parallelization, fileIn)).collect();

        int cur = 0;
        for (String[] arr : results){
           for (String s : arr){
               datasetFilePaths[cur] = s;
               cur++;
           }
        }
    }

    /**
     * Called from generateTDatasets(), this function generates and mines randomly generated datasets
     * @param   i               an indicator of which processor
     * @param   T               the number of random datasets for the Monte Carlo estimate of p-values
     * @param   theta           the minimum frequency threshold used to mine the dataset
     * @param   strategy        the method of sampling the random dataset
     * @param   parallelization the number of cores of the machine. It is the number of random datasets
     *                          generated at the same time in the Monte Carlo estimation. Each core of
     *                          the machine computes the p-values on T/parallelization random datasets.
     * @return                  returns an array of file paths where the mined datasets are located
     */
    public static String[] parallelGenDataset(int i, int T, double theta, String strategy, int parallelization, String fileIn
                                          ) throws IOException, ClassNotFoundException {
        String file = fileIn.split("\\.")[0];
        Random r = new Random();
        //Generate random dataset
        Utils.Dataset datasetOrigin = new Utils.Dataset();
        Int2ObjectOpenHashMap<Utils.Pair> positionOrigin = new Int2ObjectOpenHashMap<>();
        loadDataset(fileIn, positionOrigin, datasetOrigin);


        int start = T * (i - 1) / parallelization;
        int end = T * i / parallelization;
        String[] datasetFilePaths = new String[end - start];
        for(int j=start;j<end;j++){
            Utils.Dataset datasetRandom = Utils.generateDataset(datasetOrigin, positionOrigin, strategy, r);

            String fileRandom = file + "_random_" + j + ".txt";
            String fileMined = file + "_random_" + j + "_mined.txt";

            //Mine Dataset
            writeDataset(fileRandom, datasetRandom);
            mining(fileRandom, fileMined, theta);
            new File(fileRandom).delete();
            datasetFilePaths[j-start] = fileMined;
        }
        return datasetFilePaths;
    }

    /**
     * Executes SPEck algorithm. It reads the starting dataset from file and after the computation
     * it stored the SFSF found in teh output file.
     * @param   P               the number of random datasets used for the WY method
     * @param   T               the number of random datasets for the Monte Carlo estimate of p-values
     * @param   parallelization the number of cores of the machine. It is the number of random datasets
     *                          generated at the same time in the Monte Carlo estimation. Each core of
     *                          the machine computes the p-values on T/parallelization random datasets.
     * @param theta             the minimum frequency threshold used to mine the dataset
     */
    void execute(int P, int T, int parallelization, double theta) throws IOException, ClassNotFoundException, InterruptedException {
        String file = fileIn.split("\\.")[0];
        String fileRandom = file + "_random.txt";
        String fileMinned = file + "_mined.txt";
        // loads the input dataset in the data structures provided in input
        loadDataset(fileIn,positionOrigin,datasetOrigin);
        // fixes the seed for the first random generator
        Random r = new Random();
        System.err.println("Generating t datasets...");
        generateTDatasets(T, theta, parallelization);
        double[] minPvalue = new double[P];
        // for all the P datasets of the WY method
        System.err.println("Calculating p values for corrected threshold in spark...");
        for(int j=0;j<P;j++) {
            datasetRandom = Utils.generateDataset(datasetOrigin, positionOrigin, strategy, r);
            // writes the actual random dataset generated
            writeDataset(fileRandom,datasetRandom);
            // mines the actual random dataset generated
            mining(fileRandom, fileMinned, theta);
            // computes in parallel the p-values of the actual random dataset
            int[] pValueInt = computePValues(T,parallelization,fileMinned);
            // finds the minimum of the p-values computed from the actual random dataset
            int min = Integer.MAX_VALUE;
            for (int i : pValueInt) {
                if (min > i) min = i;
            }
            minPvalue[j] = (1 + min) / (T * 1. + 1);
        }
        System.err.println("Combining spark results...");
        // sorts the P minimum p-values
        Arrays.sort(minPvalue);
        // computes the corrected threshold
        correctedThreshold = minPvalue[(int)(P*0.05)];
        // if the corrected threshold is greater than the minimum possible value
        if(correctedThreshold!=1/(T * 1. + 1)){
            System.err.println("Calculating final p-values...");
            // mines the starting dataset
            mining(fileIn, fileMinned, theta);
            file = fileIn;
            // computes the p-values of the starting dataset in parallel
            int[] pValueInt = computePValues(T,parallelization,fileMinned);
            // reads and stores the FSP mined from the starting dataset
            FileReader fr = new FileReader(fileMinned);
            BufferedReader br = new BufferedReader(fr);
            String line = br.readLine();
            ObjectArrayList<String> fsp = new ObjectArrayList<>();
            while (line != null) {
                String[] splitted = line.split(" #SUP: ");
                fsp.add(splitted[0]);
                fspSup.put(splitted[0], Integer.parseInt(splitted[1]));
                line = br.readLine();
            }
            br.close();
            fr.close();
            this.numFsp = fsp.size();
            for (int k = 0; k < pValueInt.length; k++) {
                sfsp.add(new Utils.PairT<String, Double>(fsp.get(k), (1 + pValueInt[k]) / (T * 1. + 1)));
            }
            // sort the sfsp
            sfsp.sort(Comparator.comparing(o -> o.y));
            // writes the sfsp in the output file with their support and p-value
            FileWriter fw = new FileWriter(fileOut);
            BufferedWriter bw = new BufferedWriter(fw);
            int numSFSP = 0;
            for (Utils.PairT<String, Double> currPair : sfsp) {
                // if the p-value is lower than the corrected threshold
                if(currPair.y<correctedThreshold){
                    numSFSP++;
                    bw.write(currPair.x + " #SUP: " + fspSup.getInt(currPair.x) + " #P-VALUE: " + currPair.y + "\n");
                }
            }
            bw.close();
            fw.close();
            this.numSfsp = numSFSP;
        } else {
            this.numSfsp = 0;
            this.numFsp = -1;
        }
        // deletes the files generated
        File fileR = new File(fileRandom);
        fileR.delete();
        File fileM = new File(fileMinned);
        fileM.delete();

        for (String fp : datasetFilePaths) new File(fp).delete();
    }

    /**
     * Main class of SPEck:
     * Input parameters:
     * 0:   dataset name. A dataset in SPMF format, es: FIFA,BIBLE,SIGN,BIKE,LEVIATHAN.
     *      It must be in the data folder. It is possible to include subfolders es:
     *      subfolder1/subfolder2/dataset.
     * 1:   the number of random datasets used for the WY method
     * 2:   the number of random datasets for the Monte Carlo estimate of p-values
     * 3:   the minimum frequency threshold
     * 4:   the number of cores of the machine
     * 5:   the strategy used by SPEck to generate random datasets (in camelCase)--
     *            itemsetsSwaps,
     *            completePerm,
     *            sameSizePerm,
     *            sameSizeSwaps,
     *            sameFreqSwaps,
     *            sameSizeSeqSwaps
     * 6:   "t" to print json file, otherwise "f"
     */
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String dataset = args[0];
        int P = Integer.parseInt(args[1]);
        int T = Integer.parseInt(args[2]);
        double theta = Double.parseDouble(args[3]);
        int parallelization = Integer.parseInt(args[4]);
        String strategy = args[5];
        boolean createJson = args[6].equals("t");
        String fileIn = "data/"+dataset+".txt";
        String fileOut = "data/"+dataset+"_SFSP.txt";
        long start = System.currentTimeMillis();
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("SPEck").set("spark.executor.memory","5g").set("spark.driver.memory","5g").set("spark.executor.heartbeatInterval","10000000").set("spark.network.timeout", "10000000");
        JavaSparkContext scc = new JavaSparkContext(sparkConf);
        PropertyConfigurator.configure("log4j.properties");
        SPEck speck = new SPEck(fileIn,fileOut,scc, strategy);
        speck.execute(P,T,parallelization,theta);
        scc.stop();
        Long timeElapsed = System.currentTimeMillis()-start;

        if (!createJson) System.exit(0);
        int numItemsets = Utils.getNumItemsets(speck.datasetOrigin);
        int numTransactions = speck.datasetOrigin.size();
        JSONObject output = new JSONObject();
        JSONObject confs = new JSONObject();
        JSONObject results = new JSONObject();
        confs.put("P", P);
        confs.put("T", T);
        confs.put("strategy", strategy);
        confs.put("theta", theta);
        confs.put("procs", parallelization);
        confs.put("dataset", dataset);
        confs.put("numItemsets", numItemsets);
        confs.put("numTransactions", numTransactions);
        results.put("runtime", timeElapsed); //More detailed runtimes
        results.put("numSfsp", speck.numSfsp);
        results.put("numFsp", speck.numFsp);
        results.put("correctedThreshold", speck.correctedThreshold);

        //Make list of sfsp
        JSONObject sfsp = new JSONObject();
        for (Utils.PairT<String, Double> currPair : speck.sfsp) {
            // if the p-value is lower than the corrected threshold
            if(currPair.y<speck.correctedThreshold){
                JSONObject patternInfo = new JSONObject();
                patternInfo.put("sup", speck.fspSup.getInt(currPair.x));
                patternInfo.put("pValue", currPair.y);
                sfsp.put(currPair.x, patternInfo);
            }
        }

        results.put("sfsp", sfsp);
        output.put("confs", confs);
        output.put("results", results);

        System.out.println(output.toString(4));
    }
}
