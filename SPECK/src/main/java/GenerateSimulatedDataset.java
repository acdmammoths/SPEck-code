import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateSimulatedDataset {
    public static void genDataset(String fileout, int numTrans, int avgTransLen, int avgItemsetLen) throws IOException {
        FileWriter fw = new FileWriter(fileout + "/sim_" + numTrans + "_" + avgTransLen + "_" + avgItemsetLen + ".txt");
        Random r = new Random();
        for (int line = 0 ; line < numTrans; line++) {
            int numItemsets = (int) Math.max((r.nextGaussian() * avgTransLen * 0.3 + avgTransLen), 1);
            // Generate transaction
            for (int itemset = 0; itemset < numItemsets; itemset++) {
                int numItems = (int) Math.max((r.nextGaussian() * avgItemsetLen * 0.6 + avgItemsetLen), 1);
                // Generate itemset
                for (int item = 0; item < numItems; item++) {
                    fw.write(r.nextInt(100) + " ");
                }
                fw.write("-1 ");
            }
            fw.write("-2\n");
        }
        fw.close();
    }

    public static void main(String[] args) throws IOException {
        genDataset("data/sim_datasets", 1000, 50, 1);
        genDataset("data/sim_datasets", 3162, 50, 1);
        genDataset("data/sim_datasets", 10000, 50, 1);
        genDataset("data/sim_datasets", 31623, 50, 1);
        genDataset("data/sim_datasets", 100000, 50, 1);
    }
}
