import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class RuntimeExperiment {
    public static JSONObject parseConfFile(String confFilePath) throws IOException {
        File file = new File(confFilePath);
        String content = FileUtils.readFileToString(file, "utf-8");
        return new JSONObject(content);
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // Parse JSON File
        JSONObject confs = parseConfFile(args[0]);
        JSONArray strategies = (JSONArray) confs.get("strategies");
        JSONArray thetas = (JSONArray) confs.get("thetas");
        String dataset = confs.getString("dataset");
	    String filepath = confs.getString("datasetFilepath");
	    String outdir = confs.getString("outdir");
        boolean simulatedData = confs.getString("simulated").equals("true");
        int reps = confs.getInt("reps");

        Int2ObjectOpenHashMap<Utils.Pair> positionOrigin = new Int2ObjectOpenHashMap<>();
        Utils.Dataset datasetOrigin = new Utils.Dataset();
        // Load dataset
        SPEck.loadDataset(filepath, positionOrigin, datasetOrigin);

        JSONObject results = new JSONObject();
        results.put("dataset", dataset);
        results.put("numItemsets", positionOrigin.size());
        results.put("numTransactions", datasetOrigin.size());

        for (Object o1 : strategies) {
            String strategy = (String) o1;
            System.out.println(strategy);
            JSONObject stratResults = new JSONObject();
            for (Object o2 : thetas) {
                double theta = Double.parseDouble((String) o2);
                System.out.println("\t" + theta);
                JSONArray genTimes = new JSONArray();
                JSONArray miningTimes = new JSONArray();
                for (int i = 0; i < reps; i ++) {
                    System.out.println("\t\t" + i);
                    Utils.Dataset datasetRandom = (Utils.Dataset) Utils.deepCopy(datasetOrigin);
                    Random r = new Random();
                    String fileRandom = "data/" +dataset+ "_random.txt";
                    String fileMined = "data/" +dataset+ "_mined.txt";
                    long genTime = -1;
                    long miningTime = -1;
                    switch (strategy) {
                        case "itemsetsSwaps":
                            genTime = RandomDatasets.itemsetsSwaps(2 * positionOrigin.size(), r, positionOrigin, datasetRandom);
                            break;
                        case "completePerm":
                            genTime = RandomDatasets.completePerm(datasetRandom, datasetOrigin);
                            break;
                        case "sameSizePerm":
                            genTime = RandomDatasets.sameSizePerm(datasetRandom, datasetOrigin);
                            break;
                        case "sameSizeSwaps":
                            genTime = RandomDatasets.sameSizeSwaps(2 * positionOrigin.size(), r, positionOrigin, datasetRandom, datasetOrigin);
                            break;
			            case "sameFreqSwaps":
			                genTime = RandomDatasets.sameFreqSwaps(datasetRandom, positionOrigin, 2 * positionOrigin.size(), r);
			                break;
                        case "sameSizeSeqSwaps":
                            genTime = RandomDatasets.sameSizeSeqSwaps(2 * positionOrigin.size(), r, datasetRandom);
                            break;
                    }
                    genTimes.put(genTime);
                    SPEck.writeDataset(fileRandom, datasetRandom);
                    if (!simulatedData) {
                        long start = System.nanoTime();
                        SPEck.mining(fileRandom, fileMined, theta);
                        miningTime = System.nanoTime() - start;
                        miningTimes.put(miningTime);
                    } else {
                        miningTimes.put(-1);
                    }
                }
                JSONObject res = new JSONObject();
                res.put("mining times", miningTimes);
                res.put("generation times", genTimes);
                stratResults.put(String.valueOf(theta), res);
            }
            results.put(strategy, stratResults);

        }
        FileWriter fw = new FileWriter(outdir + "runtime/" +dataset +".json");
        fw.write(results.toString(4));
        fw.close();

    }
}
