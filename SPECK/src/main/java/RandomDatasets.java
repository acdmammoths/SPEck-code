// import appropriate third-party packages
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

// import appropriate standard java packages
import java.io.IOException;
import java.util.Collections;
import java.util.Random;
import java.util.stream.DoubleStream;

/**
 * This class includes several methods for generating random transactional
 * datasets which are to be used in the SPEck algorithm (SPEck.java).
 */
public class RandomDatasets {

    /**
     * It generates a random dataset performing a series of itemset swaps numberOfSwaps times.
     * It returns the time required for the execution of the above-mentioned task.
     *
     * @param numberOfSwaps <Integer> the number of itemsetsSwaps to perform
     * @param rand          <Random> the random generator used to choose the swaps
     * @param position      <Int2ObjectOpenHashMap<Utils.Pair>> the hashmap that contains the positions of the dataset as used in loadDataset method of SPEck
     * @param dataset       <Utils.Dataset> the list of lists of itemset objects that comprises the random dataset after the execution
     * @return the time required to generate the random dataset
     */
    public static long itemsetsSwaps(
            int numberOfSwaps,
            Random rand,
            Int2ObjectOpenHashMap<Utils.Pair> position,
            Utils.Dataset dataset)
    {
        // starting the timer
        long start = System.nanoTime();

        // repeating the process of swapping itemsets
        for (int i = 0; i < numberOfSwaps; i++) {
            // getting the positions randomly
            Utils.Pair p1 = position.get(rand.nextInt(position.size()));
            Utils.Pair p2 = position.get(rand.nextInt(position.size()));

            // getting access to the itemsets to randomly selected positions p1 and p2
            Utils.Itemset i1 = dataset.get(p1.x).get(p1.y);
            Utils.Itemset i2 = dataset.get(p2.x).get(p2.y);

            // swapping the itemsets
            dataset.get(p1.x).set(p1.y, i2);
            dataset.get(p2.x).set(p2.y, i1);
        }
        // stopping the timer
        long end = System.nanoTime();

        // return the time taken to generate a random dataset
        return end - start;
    }

    /**
     * It generates a random dataset permuting all itemsets while maintaining transaction lengths.
     * It returns the time required for the generation.
     *
     * @param datasetRandom <Utils.Dataset> the list of lists of itemset objects that comprises the random dataset after the execution
     * @param datasetOrigin <Utils.Dataset> storing the observed dataset as a list of list of itemset objects
     * @return the time required to generate the random dataset
     */

    public static long completePerm(
            Utils.Dataset datasetRandom,
            Utils.Dataset datasetOrigin)
    {
        // start the timer
        long start = System.nanoTime();

        // initialize one-dimensional list to store the newly made dataset
        ObjectArrayList<Utils.Itemset> dataset_list = new ObjectArrayList<>();

        // iterating through transactions and itemsets and add to dataset_list
        for (ObjectArrayList<Utils.Itemset> trans : datasetOrigin) {
            for (Utils.Itemset i : trans) {
                dataset_list.add(i);
            }
        }

        // randomly shuffle/permute the itemsets in dataset_list
        Collections.shuffle(dataset_list);

        // converting dataset_list to a random dataset
        Utils.buildDatasetFromList(dataset_list, datasetRandom);

        // stop the timer
        long end = System.nanoTime();

        // return the time taken for the process
        return end - start;
    }

    /**
     * It generates a random dataset permuting all itemsets of the same length thus maintaining item-lengths.
     * It returns the time required for the generation.
     *
     * @param datasetRandom <Utils.Dataset> the list of lists of itemset objects that comprises the random dataset after the execution
     * @param datasetOrigin <Utils.Dataset> the observed list of lists of itemset objects
     * @return the time required to generate the random dataset
     */

    public static long sameSizePerm(
            Utils.Dataset datasetRandom,
            Utils.Dataset datasetOrigin
    ) throws IOException, ClassNotFoundException {
        // start the timer
        long start = System.nanoTime();

        // temporarily store the number of itemsets in original dataset
        int m = Utils.getNumItemsets(datasetOrigin);

        // iterating through transactions and itemsets and add to dataset_list
        ObjectArrayList<Utils.Itemset> datasetList = new ObjectArrayList<>();
        for (ObjectArrayList<Utils.Itemset> trans : datasetOrigin) {
            for (Utils.Itemset i : trans) {
                datasetList.add(i);
            }
        }

        // initializing the newDatasetList
        ObjectArrayList<Utils.Itemset> newDatasetList = new ObjectArrayList<>();
        for (int i = 0; i < m; i++) {
            newDatasetList.add(null);
        }

        // initialize a cardinality list
        Int2ObjectOpenHashMap<IntArrayList> cardList = Utils.getCardList(datasetOrigin);

        //shuffle the positions of each element in cardinality list
        for (IntArrayList cardList_i : cardList.values()) {
            IntArrayList q = (IntArrayList) Utils.deepCopy(cardList_i);
            Collections.shuffle(q);

            // assign each itemset to its permuted position
            for (int j = 0; j < q.size(); j++) {
                newDatasetList.set(q.getInt(j), datasetList.get(cardList_i.getInt(j)));
            }
        }

        // converting newDatasetList to a random dataset
        Utils.buildDatasetFromList(newDatasetList, datasetRandom);

        //shuffle each transaction in datasetRandom
        for (ObjectArrayList<Utils.Itemset> trans : datasetRandom) {
            Collections.shuffle(trans);
        }

        // stopping the timer
        long end = System.nanoTime();

        // retrurn the time required to do the process
        return end - start;
    }


    /**
     * It generates a random dataset performing a series of 2m swaps of itemsets of the same size.
     * It returns the time required for the generation.
     *
     * @param numSwaps         <Integer> the number of swaps needed to be done
     * @param rand             <Random> the random generator used to choose the swaps
     * @param positions        <Int2ObjectOpenHashMap<Utils.Pair>> the hashmap that contains the positions of the dataset (See loadDataset method)
     * @param datasetRandom    <Utils.Dataset> the list of lists of itemset objects that comprises the random dataset after the execution
     * @param datasetOrigin    <Utils.Dataset> the observed list of lists of itemset objects
     * @return the time required to generate the random dataset
     */
    public static long sameSizeSwaps(
            int numSwaps,
            Random rand,
            Int2ObjectOpenHashMap<Utils.Pair> positions,
            Utils.Dataset datasetRandom,
            Utils.Dataset datasetOrigin)
    {
        // start the timer
        long start = System.nanoTime();

        // initialize a cardinality list
        Int2ObjectOpenHashMap<IntArrayList> cardList = Utils.getCardList(datasetOrigin);

        // select itemsets to be swapped according to weighted probabilities from cardinality list
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

        // perform the swaps numSwaps times
        for (int i = 0; i < numSwaps; i++) {
            // j is the element in the cardinality list to choose two itemsets from
            int j = Utils.randomWeightedChoice(probs);

            // choosing the two itemsets accordingly
            int a = rand.nextInt(cardList.get(j).size());
            int b = rand.nextInt(cardList.get(j).size());

            //swap two itemsets at hand by first getting their positions
            Utils.Pair p1 = positions.get(cardList.get(j).getInt(a));
            Utils.Pair p2 = positions.get(cardList.get(j).getInt(b));
            Utils.Itemset i1 = datasetRandom.get(p1.x).get(p1.y);
            Utils.Itemset i2 = datasetRandom.get(p2.x).get(p2.y);
            datasetRandom.get(p1.x).set(p1.y, i2);
            datasetRandom.get(p2.x).set(p2.y, i1);
        }

        //shuffle each transaction in datasetRandom
        for (ObjectArrayList<Utils.Itemset> trans : datasetRandom) {
            Collections.shuffle(trans);
        }

        // stopping the timer
        long end = System.nanoTime();

        // return the time required to do the process
        return end - start;
    }

    /**
     * It generates a random dataset performing a series of 2m swaps of itemsets. It only performs swaps that maintain
     * the frequency of all itemsets in the original dataset.
     * It returns the time required for the generation.
     *
     * @param datasetRandom <Utils.Dataset> the list of lists of itemset objects that comprises the random dataset after the execution
     * @param positions     <Int2ObjectOpenHashMap<Utils.Pair>> the hashmap that contains the positions of the dataset (See loadDataset method)
     * @param numSwaps      <Integer> the number of swaps to be performed in the generation
     * @param rand          <Random> the random generator used to choose the swaps
     * @return the time required to generate the random dataset
     */
    public static long sameFreqSwaps(Utils.Dataset datasetRandom,
                                     Int2ObjectOpenHashMap<Utils.Pair> positions,
                                     int numSwaps,
                                     Random rand)
    {
        // start the timer
        long start = System.nanoTime();

        // data structure mapping each unique itemset to id to the list of transactions it appears in
        Int2ObjectOpenHashMap<Int2IntOpenHashMap> idToTransactionMap = new Int2ObjectOpenHashMap<>();

        // initializing the ID to Transaction map
        for (int t = 0; t < datasetRandom.size(); t++) {
            for (Utils.Itemset i : datasetRandom.get(t)) {
                if (!(idToTransactionMap.containsKey(i.getId()))) {
                    idToTransactionMap.put(i.getId(), new Int2IntOpenHashMap());
                }
                if (idToTransactionMap.get(i.getId()).containsKey(t)) {
                    idToTransactionMap.get(i.getId()).put(t, 1);
                } else idToTransactionMap.get(i.getId()).addTo(t, 1);
            }
        }

        // performing swaps procedure numSwaps times
        int i = 0;
        while (i < numSwaps) {
            // get the positions and thus itemsets randomly
            Utils.Pair p1 = positions.get(rand.nextInt(positions.size()));
            Utils.Pair p2 = positions.get(rand.nextInt(positions.size()));
            Utils.Itemset i1 = datasetRandom.get(p1.x).get(p1.y);
            Utils.Itemset i2 = datasetRandom.get(p2.x).get(p2.y);

            //only perform swap if original frequencies are maintained
            if (Utils.isValidSwap(i1, i2, idToTransactionMap)) {

                datasetRandom.get(p1.x).set(p1.y, i2);
                datasetRandom.get(p2.x).set(p2.y, i1);
                i1.setTid(p2.x);
                i2.setTid(p1.x);

                //update idToTransactionMap map
                idToTransactionMap.get(i1.getId()).addTo(i1.getTid(), -1);
                idToTransactionMap.get(i2.getId()).addTo(i2.getTid(), -1);
                if (idToTransactionMap.get(i1.getId()).containsKey(i2.getTid())) {
                    idToTransactionMap.get(i1.getId()).addTo(i2.getTid(), 1);
                } else idToTransactionMap.get(i1.getId()).put(i2.getTid(), 1);
                if (idToTransactionMap.get(i2.getId()).containsKey(i1.getTid())) {
                    idToTransactionMap.get(i2.getId()).addTo(i1.getTid(), 1);
                } else idToTransactionMap.get(i2.getId()).put(i1.getTid(), 1);

                // only incremented i here because we actually did a swap here
                i++;
            }
        }

        //shuffle each transaction in datasetRandom
        for (ObjectArrayList<Utils.Itemset> trans : datasetRandom) {
            Collections.shuffle(trans);
        }

        // stopping the timer
        long end = System.nanoTime();

        // retrurn the time required to do the process
        return end - start;
    }

    /**
     * It generates a random dataset performing a series of 2m swaps of sequences (sets of one or two itemsets) with the same item length.
     * It returns the time required for the generation.
     *
     * @param numSwaps      <Integer> the number of swaps to be performed in the generation
     * @param rand          <Random> the random generator used to choose the swaps
     * @param datasetRandom <Utils.Dataset> the list of lists of itemset objects that comprises the random dataset after the execution
     * @return the time required to generate the random dataset
     */
    public static long sameSizeSeqSwaps(int numSwaps,
                                        Random rand,
                                        Utils.Dataset datasetRandom)
    {
        // start the timer
        long start = System.nanoTime();

        // initialize a cardinality list
        Int2ObjectOpenHashMap<ObjectArrayList<Utils.Sequence>> cardList = Utils.getCardListPairs(datasetRandom);

        // perform the procedure numSwaps times
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

            // standardizing the probabilities to be in the range of 0 to 1
            double sum = DoubleStream.of(probs).sum();
            for (int j = 1; j < probs.length; j++) {
                probs[j] /= sum;
            }

            // get two random sequences a and b
            int k = Utils.randomWeightedChoice(probs);
            int a = rand.nextInt(cardList.get(k).size());
            int b = rand.nextInt(cardList.get(k).size());
            if (a == b) continue;
            Utils.Sequence sA = cardList.get(k).get(a);
            Utils.Sequence sB = cardList.get(k).get(b);

            // remove the corresponding sequences from cardinality list
            Utils.removeCardLists(sA, sB, cardList, datasetRandom);

            // swap the corresponding sequences at hand
            Utils.swapSeq(sA, sB, datasetRandom);

            // update the cardinality lists with the swapped sequences
            Utils.updateCardLists(sA, sB, cardList, datasetRandom);
        }
        // shuffle each transaction in datasetRandom
        for (ObjectArrayList<Utils.Itemset> trans : datasetRandom) {
            Collections.shuffle(trans);
        }

        // stopping the timer
        long end = System.nanoTime();
        // return the time required to do the process
        return end - start;
    }
}
