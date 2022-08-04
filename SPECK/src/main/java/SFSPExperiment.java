import org.apache.commons.io.FileUtils;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;

public class SFSPExperiment {
    public static JSONObject parseConfFile(String confFilePath) throws IOException {
        File file = new File(confFilePath);
        String content = FileUtils.readFileToString(file, "utf-8");
        return new JSONObject(content);
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String confFilePath = args[0];
        JSONObject json = parseConfFile(confFilePath);

        JSONArray P = (JSONArray) json.get("P");
        JSONArray T = (JSONArray) json.get("T");
        JSONArray thetas = (JSONArray) json.get("thetas");
        String fwer = json.getString("fwer");
        JSONArray strategies = (JSONArray) json.get("strategies");
        String reps = json.getString("reps");
        String procs = json.getString("procs");
        String outdir = json.getString("outdir");
        String dataset = json.getString("dataset");
        for (Object o1 : strategies) {
            String strategy = (String) o1;
            System.out.println(dataset + " - " + strategy);
            for (Object o2 : P) {
                String p = (String) o2;
                System.out.println("  " + p);
                for (Object o3 : T) {
                    String t = (String) o3;
                    System.out.println("    " + t);
                    for (Object o4 : thetas) {
                        String theta = (String) o4;
                        System.out.println("      " + theta);
                            String file = outdir +"sfsp/"+dataset+"_"+strategy+"_"+p+"_"+t+"_"+theta+"_";//+rep;
                            String[] expArgs = {dataset, p, t, theta, fwer, procs, strategy, "t"};

                            PrintStream fileOut = new PrintStream(file + ".json");
                            System.setOut(fileOut);

                            PrintStream fileErr = new PrintStream(file + ".log");
                            System.setErr(fileErr);

                            SPEck.main(expArgs);

                            System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
                    }
                }
            }
        }
    }
}