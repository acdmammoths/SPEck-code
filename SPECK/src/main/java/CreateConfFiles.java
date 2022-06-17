import org.json.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class CreateConfFiles {
    public static void main(String[] args) throws IOException {
        boolean simulatedData = false;
        
        int numProcs = 0;
        try {
            numProcs = Integer.parseInt(args[0]);
        } catch (Exception e) {
            System.out.println("Invalid number of processors");
            System.exit(1);
        }
        if (args.length > 1) {
            if (args[1].equals("-sim")) {
                simulatedData = true;
            }
        }
        
        String[] strategies = {"completePerm","itemsetsSwaps","sameSizePerm","sameSizeSwaps", "sameFreqSwaps"};
        String[] P = {"100"};
        String[] T = {"10048"};
        String[] datasets;
        if (simulatedData) {
            File dir = new File("data/sim_datasets/");
            datasets = dir.list();
            assert datasets != null;
            for (int i = 0; i < datasets.length; i++) {
                datasets[i] = datasets[i].split("\\.")[0];
            }
        } else {
            datasets = new String[]{"BIBLE","SIGN", "BIKE", "LEVIATHAN", "FIFA"};
        }
        for (String dataset : datasets) {
            if (dataset.equals("")) continue;
            String[] thetas;

            switch (dataset) {
                case "SIGN":
                    thetas = new String[]{"0.4"};
                    break;
                case "BIBLE":
                    thetas = new String[]{"0.1"};
                    break;
                case "BIKE":
                    thetas = new String[]{"0.025"};
                    break;
                case "LEVIATHAN":
                    thetas = new String[]{"0.15"};
                    break;
                case "FIFA":
                    thetas = new String[]{"0.275"};
                    break;
                default:
                    thetas = new String[]{"0.2"};
                    break;
            }
            String confFilePath = "confs/" + dataset +".json";

            FileWriter fw = new FileWriter(confFilePath);
            JSONObject json = new JSONObject();
            json.put("dataset", dataset);
            json.put("P", P);
            json.put("T", T);
            json.put("strategies", strategies);
            json.put("procs", String.valueOf(numProcs));
            if (simulatedData) {
                json.put("datasetFilepath", "data/sim_datasets/"+dataset+".txt");
            } else {
                json.put("datasetFilepath", "data/"+dataset+".txt");
            }
            json.put("thetas", thetas);
            json.put("reps","20");
            json.put("outdir", "results/");
            json.put("simulated", simulatedData ? "true":"false");

            fw.write(json.toString(4));
            fw.close();
            System.out.println("DONE!");
        }
    }
}
