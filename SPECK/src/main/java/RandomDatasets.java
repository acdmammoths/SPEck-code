import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.stream.DoubleStream;

/**
 * This class includes several methods for generating random transactional
 * datasets to be used in the SPEck algorithm.
 */

public class RandomDatasets {

    /**
     * It generates a random dataset performing a series of numberOfSwaps itemsetsSwaps.
     * It returns the time required for the generation.
     *
     * @param numberOfSwaps the number of itemsetsSwaps to perform
     * @param rand          the random generator used to choose the swaps
     * @param position      the hashmap that contains the positions of the dataset (See loadDataset method)
     * @param dataset       the list of lists of itemset objects that comprises the random dataset after the execution
     * @return the time required to generate the random dataset
     */
    public static long itemsetsSwaps(
            int numberOfSwaps,
            Random rand,
            Int2ObjectOpenHashMap<Utils.Pair> position,
            Utils.Dataset dataset) {
        long start = System.nanoTime();
        for (int i = 0; i < numberOfSwaps; i++) {
            Utils.Pair p1 = position.get(rand.nextInt(position.size()));
            Utils.Pair p2 = position.get(rand.nextInt(position.size()));
            Utils.Itemset i1 = dataset.get(p1.x).get(p1.y);
            Utils.Itemset i2 = dataset.get(p2.x).get(p2.y);
            dataset.get(p1.x).set(p1.y, i2);
            dataset.get(p2.x).set(p2.y, i1);
        }
        return System.nanoTime() - start;
    }

    /**
     * It generates a random dataset permuting all itemsets while maintaining transaction lengths.
     * It returns the time required for the generation.
     *
     * @param datasetRandom the list of lists of itemset objects that comprises the random dataset after the execution
     * @param datasetOrigin the original list of lists of itemset objects
     * @return the time required to generate the random dataset
     */

    public static long completePerm(
            Utils.Dataset datasetRandom,
            Utils.Dataset datasetOrigin) {
        long start = System.nanoTime();
        //make dataset into one-dimensional list
        ObjectArrayList<Utils.Itemset> dataset_list = new ObjectArrayList<>();
        for (ObjectArrayList<Utils.Itemset> trans : datasetOrigin) {
            for (Utils.Itemset i : trans) {
                dataset_list.add(i);
            }
        }
        Collections.shuffle(dataset_list);
        Utils.buildDatasetFromList(dataset_list, datasetRandom);
        return System.nanoTime() - start;
    }

    /**
     * It generates a random dataset permuting all itemsets of the same length thus maintaining item-lengths.
     * It returns the time required for the generation.
     *
     * @param datasetRandom the list of lists of itemset objects that comprises the random dataset after the execution
     * @param datasetOrigin the original list of lists of itemset objects
     * @return the time required to generate the random dataset
     */

    public static long sameSizePerm(
            Utils.Dataset datasetRandom,
            Utils.Dataset datasetOrigin) throws IOException, ClassNotFoundException {

        long start = System.nanoTime();
        int m = Utils.getNumItemsets(datasetOrigin);
        //make dataset into a one-dimensional list
        ObjectArrayList<Utils.Itemset> datasetList = new ObjectArrayList<>();
        for (ObjectArrayList<Utils.Itemset> trans : datasetOrigin) {
            for (Utils.Itemset i : trans) {
                datasetList.add(i);
            }
        }
        //newDatasetList will be permuted to generate datasetRandom
        ObjectArrayList<Utils.Itemset> newDatasetList = new ObjectArrayList<>();
        for (int i = 0; i < m; i++) {
            newDatasetList.add(null);
        }
        Int2ObjectOpenHashMap<IntArrayList> cardList = Utils.getCardList(datasetOrigin);
        //shuffle the positions of each element in cardinality list
        for (IntArrayList cardList_i : cardList.values()) {
            IntArrayList q = (IntArrayList) Utils.deepCopy(cardList_i);
            Collections.shuffle(q);
            //assign each itemset to its permuted position
            for (int j = 0; j < q.size(); j++) {
                newDatasetList.set(q.getInt(j), datasetList.get(cardList_i.getInt(j)));
            }
        }
        Utils.buildDatasetFromList(newDatasetList, datasetRandom);
        //shuffle each transaction in datasetRandom
        for (ObjectArrayList<Utils.Itemset> trans : datasetRandom) Collections.shuffle(trans);
        return System.nanoTime() - start;
    }


    /**
     * It generates a random dataset performing a series of 2m swaps of itemsets of the same size.
     * It returns the time required for the generation.
     *
     * @param r             the random generator used to choose the swaps
     * @param positions     the hashmap that contains the positions of the dataset (See loadDataset method)
     * @param datasetRandom the list of lists of itemset objects that comprises the random dataset after the execution
     * @param datasetOrigin the original list of lists of itemset objects
     * @return the time required to generate the random dataset
     */
    public static long sameSizeSwaps(
            int numSwaps,
            Random r,
            Int2ObjectOpenHashMap<Utils.Pair> positions,
            Utils.Dataset datasetRandom,
            Utils.Dataset datasetOrigin) {
        long start = System.nanoTime();
        Int2ObjectOpenHashMap<IntArrayList> cardList = Utils.getCardList(datasetOrigin);
        //select itemsets to be swapped according to weighted probabilities from cardinality list
        IntIterator it = cardList.keySet().iterator();
        int maxCardSize = 0;
        while (it.hasNext()) {
            int m = it.nextInt();
            if (m > maxCardSize) maxCardSize = m;
        }
        double[] probs = new double[maxCardSize + 1];
        for (int i = 1; i < probs.length; i++) {
            if (cardList.containsKey(i)) {
                probs[i] = Math.pow(cardList.get(i).size(), 2);
            }
        }

        for (int i = 0; i < numSwaps; i++) {
            //j is the element in the cardinality list to choose two itemsets from
            int j = Utils.randomWeightedChoice(probs);
            int a = r.nextInt(cardList.get(j).size());
            int b = r.nextInt(cardList.get(j).size());
            //swap a and b
            Utils.Pair p1 = positions.get(cardList.get(j).getInt(a));
            Utils.Pair p2 = positions.get(cardList.get(j).getInt(b));
            Utils.Itemset i1 = datasetRandom.get(p1.x).get(p1.y);
            Utils.Itemset i2 = datasetRandom.get(p2.x).get(p2.y);
            datasetRandom.get(p1.x).set(p1.y, i2);
            datasetRandom.get(p2.x).set(p2.y, i1);
        }
        //shuffle all transactions
        for (ObjectArrayList<Utils.Itemset> trans : datasetRandom) Collections.shuffle(trans);
        return System.nanoTime() - start;
    }

    /**
     * It generates a random dataset performing a series of 2m swaps of itemsets. It only performs swaps that maintain
     * the frequency of all itemsets in the original dataset.
     * It returns the time required for the generation.
     *
     * @param datasetRandom the list of lists of itemset objects that comprises the random dataset after the execution
     * @param positions     the hashmap that contains the positions of the dataset (See loadDataset method)
     * @param numswaps      the number of swaps to be performed in the generation
     * @param r             the random generator used to choose the swaps
     * @return the time required to generate the random dataset
     */
    public static long sameFreqSwaps(Utils.Dataset datasetRandom, Int2ObjectOpenHashMap<Utils.Pair> positions, int numswaps, Random r) {
        long start = System.nanoTime();

        //data structure mapping each unique itemset to id to the list of transactions it appears in
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> tids = new Int2ObjectOpenHashMap<>();
        for (int t = 0; t < datasetRandom.size(); t++) {
            for (Utils.Itemset i : datasetRandom.get(t)) {
                if (!(tids.containsKey(i.getId()))) {
                    tids.put(i.getId(), new Int2IntOpenHashMap());
                }
                if (tids.get(i.getId()).containsKey(t)) {
                    tids.get(i.getId()).put(t, 1);
                } else tids.get(i.getId()).addTo(t, 1);
            }
        }
        int i = 0;
        while (i < numswaps) {
            Utils.Pair p1 = positions.get(r.nextInt(positions.size()));
            Utils.Pair p2 = positions.get(r.nextInt(positions.size()));
            Utils.Itemset i1 = datasetRandom.get(p1.x).get(p1.y);
            Utils.Itemset i2 = datasetRandom.get(p2.x).get(p2.y);

            //only perform swap if original frequencies are maintained
            if (Utils.isValidSwap(i1, i2, tids)) {
                datasetRandom.get(p1.x).set(p1.y, i2);
                datasetRandom.get(p2.x).set(p2.y, i1);
                i1.setTid(p2.x);
                i2.setTid(p1.x);

                //update tids map
                tids.get(i1.getId()).addTo(i1.getTid(), -1);
                tids.get(i2.getId()).addTo(i2.getTid(), -1);
                if (tids.get(i1.getId()).containsKey(i2.getTid())) {
                    tids.get(i1.getId()).addTo(i2.getTid(), 1);
                } else tids.get(i1.getId()).put(i2.getTid(), 1);
                if (tids.get(i2.getId()).containsKey(i1.getTid())) {
                    tids.get(i2.getId()).addTo(i1.getTid(), 1);
                } else tids.get(i2.getId()).put(i1.getTid(), 1);
                i++;
            }
        }
        //shuffle all transactions
        for (ObjectArrayList<Utils.Itemset> trans : datasetRandom) Collections.shuffle(trans);
        return System.nanoTime() - start;
    }

    /**
     * It generates a random dataset performing a series of 2m swaps of sequences (sets of one or two itemsets) with the same item length.
     * It returns the time required for the generation.
     *
     * @param numSwaps      the number of swaps to be performed in the generation
     * @param r             the random generator used to choose the swaps
     * @param datasetRandom the list of lists of itemset objects that comprises the random dataset after the execution
     * @return the time required to generate the random dataset
     */

    public static long sameSizeSeqSwaps(int numSwaps,
                                        Random r,
                                        Utils.Dataset datasetRandom) {
        long start = System.nanoTime();
        Int2ObjectOpenHashMap<ObjectArrayList<Utils.Sequence>> cardList = Utils.getCardListPairs(datasetRandom);

        for (int i = 0; i < numSwaps; i++) {

            //select sequences to be swapped (a, b) according to weighted probabilities from cardinality list
            double[] probs = new double[cardList.size() + 1];
            for (int j = 1; j < probs.length; j++) {
                try {
                    probs[j] = Math.pow(cardList.get(j).size(), 2);
                } catch (NullPointerException e) {
                    probs[j] = 0;
                }
            }
            double sum = DoubleStream.of(probs).sum();
            for (int j = 1; j < probs.length; j++) {
                probs[j] /= sum;
            }
            int k = Utils.randomWeightedChoice(probs);
            int a = r.nextInt(cardList.get(k).size());
            int b = r.nextInt(cardList.get(k).size());
            if (a == b) continue;
            Utils.Sequence sA = cardList.get(k).get(a);
            Utils.Sequence sB = cardList.get(k).get(b);

            Utils.removeCardLists(sA, sB, cardList, datasetRandom);

            Utils.swapSeq(sA, sB, datasetRandom);

            Utils.updateCardLists(sA, sB, cardList, datasetRandom);
        }
        //shuffle all transactions
        for (ObjectArrayList<Utils.Itemset> trans : datasetRandom) {
            Collections.shuffle(trans);
        }
        return System.nanoTime() - start;
    }
}

