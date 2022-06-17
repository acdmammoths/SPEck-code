import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;

public class ParseJsonFiles {
    public static void main(String[] args) throws IOException {
        String dir = args[0];
        String fileOut = args[1];
        String method = args[2];

        switch (method) {
            case "runtime":
                parseRuntimeFiles(dir, fileOut);
                break;
            case "sfsp":
                parseSfspFiles(dir, fileOut);
                break;
            case "pvalue":
                parsePValues(dir, fileOut);
        }

    }

    public static void parsePValues(String dirFP, String fileOut) throws IOException {
        FileWriter fw = new FileWriter(fileOut);
        fw.write("dataset,strategy,theta,P,T,pattern,pvalue,support\n");

        File dir = new File(dirFP);
        if (dir.isDirectory()) {
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                String fileName = file.getPath();
                String fileType = Optional.of(fileName)
                        .filter(f -> f.contains("."))
                        .map(f -> f.substring(fileName.lastIndexOf(".") + 1)).get();
                if (!fileType.equals("json")) continue;
                JSONObject json = new JSONObject( FileUtils.readFileToString(file, "utf-8"));
                JSONObject confs = json.getJSONObject("confs");
                JSONObject results = json.getJSONObject("results");

                int P = confs.getInt("P");
                int T = confs.getInt("T");
                String strategy = confs.getString("strategy");
                double theta = confs.getDouble("theta");
                String dataset = confs.getString("dataset");
                JSONObject sfsps = results.getJSONObject("sfsp");
                int numSfsp = results.getInt("numSfsp");

                if (numSfsp > 0) {
                    for (String key : sfsps.keySet()) {
                        int support = sfsps.getJSONObject(key).getInt("sup");
                        double pvalue = sfsps.getJSONObject(key).getDouble("pValue");
                        fw.write(dataset+","+strategy+","+theta+","+P+","+T+","+key+","+pvalue+","+support + "\n");
                    }
                }
            }
        }
        fw.close();
    }

    public static void parseSfspFiles(String dirFP, String fileOut) throws IOException {
        FileWriter fw = new FileWriter(fileOut);
        fw.write("dataset,strategy,theta,p,t,procs,numItemsets,numTransactions,time(ms),numFsp,numSfsp\n");
        File dir = new File(dirFP);
        if (dir.isDirectory()) {
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                String fileName = file.getPath();
                String fileType = Optional.of(fileName)
                        .filter(f -> f.contains("."))
                        .map(f -> f.substring(fileName.lastIndexOf(".") + 1)).get();
                if (!fileType.equals("json")) continue;
                JSONObject json = new JSONObject( FileUtils.readFileToString(file, "utf-8"));
                JSONObject confs = json.getJSONObject("confs");
                JSONObject results = json.getJSONObject("results");

                int P = confs.getInt("P");
                int T = confs.getInt("T");
                int procs = confs.getInt("procs");
                String strategy = confs.getString("strategy");
                double theta = confs.getDouble("theta");
                int numItemsets = confs.getInt("numItemsets");
                int numTransactions = confs.getInt("numTransactions");
                String dataset = confs.getString("dataset");
                int numFsp = results.getInt("numFsp");
                int numSfsp = results.getInt("numSfsp");
                int runtime = results.getInt("runtime");

                fw.write(dataset+","+strategy+","+theta+","+P+","+T+","
                        +procs+","+numItemsets+","+numTransactions+","+runtime+","+
                        numFsp+","+numSfsp+"\n");

            }
        }
        fw.close();
    }

    public static void parseRuntimeFiles(String runtimeDir, String fileOut) throws IOException {
        FileWriter fw = new FileWriter(fileOut);

        fw.write("dataset,strategy,theta,type,numItemsets,numTransactions,time(ns)\n");

        File dir = new File(runtimeDir);
        if (dir.isDirectory()) {
            for (File file : Objects.requireNonNull(dir.listFiles())) {

                JSONObject json = new JSONObject( FileUtils.readFileToString(file, "utf-8"));
                String dataset = json.getString("dataset");
                int numItemsets = json.getInt("numItemsets");
                int numTransactions = json.getInt("numTransactions");
                json.remove("dataset");
                json.remove("numItemsets");
                json.remove("numTransactions");

                for (Iterator<String> it = json.keys(); it.hasNext(); ) {
                    String strategy = it.next();
                    JSONObject strat = json.getJSONObject(strategy);
                    for (Iterator<String> it1 = strat.keys(); it1.hasNext(); ) {
                        String theta = it1.next();
                        JSONArray genTimes = strat.getJSONObject(theta).getJSONArray("generation times");
                        JSONArray miningTimes = strat.getJSONObject(theta).getJSONArray("mining times");

                        for (Object t : genTimes) {
                            String time = String.valueOf(t);
                            fw.write(dataset + "," + strategy + "," + theta + ",generation," +
                                    numItemsets + "," + numTransactions + "," + time + "\n");
                        }
                        for (Object t : miningTimes) {
                            String time = String.valueOf(t);
                            fw.write(dataset + "," + strategy + "," + theta + ",mining," +
                                    numItemsets + "," + numTransactions + "," + time + "\n");
                        }
                    }
                }
            }
        }
        fw.close();
    }
}
