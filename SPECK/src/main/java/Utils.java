import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.*;
import java.util.*;

public class Utils {
    /**
     * It returns the number of itemsets in the dataset
     * @param datasetOrigin   the list of lists of itemset objects that comprises the original dataset
     * @param positionOrigin    the hashmap that maps 1...m with a pair containing its transaction number and location
     *                          within the transcation
     * @param strategy          the strategy with which the random dataset should be generated
     * @param r                 random number generator
     * @return  datasetRandom   the list of lists of itemset objects that comprises the random dataset
     */
    public static Dataset generateDataset(Dataset datasetOrigin, Int2ObjectOpenHashMap<Pair> positionOrigin, String strategy, Random r) throws IOException, ClassNotFoundException {
        Utils.Dataset datasetRandom = (Utils.Dataset) Utils.deepCopy(datasetOrigin);
        int numswaps = 2 * positionOrigin.size();
        switch (strategy) {
            // uses 2*m itemsets swaps to generate a random dataset
            case "itemsetsSwaps":
                RandomDatasets.itemsetsSwaps(numswaps, r, positionOrigin, datasetRandom);
                break;
            case "completePerm":
                RandomDatasets.completePerm(datasetRandom, datasetOrigin);
                break;
            case "sameSizePerm":
                RandomDatasets.sameSizePerm(datasetRandom, datasetOrigin);
                break;
            case "sameSizeSwaps":
                RandomDatasets.sameSizeSwaps(numswaps, r, positionOrigin, datasetRandom, datasetOrigin);
                break;
            case "sameFreqSwaps":
                RandomDatasets.sameFreqSwaps(datasetRandom, positionOrigin, numswaps, r);
                break;
            case "sameSizeSeqSwaps":
                RandomDatasets.sameSizeSeqSwaps(numswaps, r, datasetRandom);
                break;

        }
        return datasetRandom;
    }

    /**
     * It returns the number of itemsets in the dataset
     * @param   dataset         the hashmap that contains object array lists (transactions) 
     *                          of itemsets(See SPEck.loadDataset method).
     * @return  numItemsets     the number of itemsets in the dataset
     */
    public static int getNumItemsets(Dataset dataset) {
        int numItemsets = 0;
        for (ObjectArrayList<Itemset> trans : dataset) numItemsets += trans.size();
        return numItemsets;
    }

    /**
     * It returns the lengths of all transactions in the dataset
     * @param   dataset         the hashmap that contains object array lists (transactions) 
     *                          of itemsets(See SPEck.loadDataset method).
     * @return  translengths    an array where each index i contains the length of the ith transaction in the dataset
     */
    public static int[] getTransLengths(Dataset dataset) {
        int[] transLengths = new int[dataset.size()];
        for (int i = 0; i < transLengths.length; i++) transLengths[i] = dataset.get(i).size();
        return transLengths;
    }

    /**
     * It returns an array of the itemlengths of every transaction
     * @param   dataset         the hashmap that contains object array lists (transactions) 
     *                          of itemsets(See SPEck.loadDataset method).
     * @return  itemLengths     an array of itemlengths
     */
    public static int[] getItemsetLengths(Dataset dataset) {
        int[] itemLengths = new int[dataset.size()];
        for (int i = 0; i < dataset.size(); i++) {
            for (int j = 0; j < dataset.get(i).size(); j++) {
                itemLengths[i] += dataset.get(i).get(j).getLength();
            }
        }
        return itemLengths;
    }

    /**
     * It returns the number of appearances of each itemset in the dataset
     * @param   dataset         the hashmap that contains object array lists (transactions) of itemsets(See SPEck.loadDataset method).
     * @return  itemsetCounts   a hashmap whose keys are the unique itemsets in dataset
     *                          and values are the number of times each itemset appears.
     */
    public static Object2IntOpenHashMap<Integer> getItemsetCounts(Dataset dataset) {
        Object2IntOpenHashMap<Integer> itemsetCounts = new Object2IntOpenHashMap<>();
        for (ObjectArrayList<Itemset> trans : dataset) {
            for (Itemset itemset : trans) {
                if (!itemsetCounts.containsKey(itemset.getId())) {
                    itemsetCounts.put(Integer.valueOf(itemset.getId()), 1);
                } else {
                    itemsetCounts.addTo(itemset.getId(), 1);
                }
            }
        }
        return itemsetCounts;
    }

    /**
     * It returns the cardinality list of the dataset
     * @param   dataset         the hashmap that contains object array lists (transactions) of itemsets(See SPEck.loadDataset method).
     * @return  cardList   a hashmap whose keys are the unique lengths of the collection of itemsets in the dataset
     *                          and values are lists containing the positions of all itemsets of that length.
     */
    public static Int2ObjectOpenHashMap<IntArrayList> getCardList(Dataset dataset){
        Int2ObjectOpenHashMap<IntArrayList> cardList = new Int2ObjectOpenHashMap<>();
        int pos = 0;
        //Add each itemset to the cardList with its length
        for (ObjectArrayList<Itemset> trans : dataset){
            for (Itemset i : trans){
                int len = i.getLength();
                if (!(cardList.containsKey(len))) {
                    cardList.put(len, new IntArrayList());
                }
                cardList.get(len).add(pos);
                pos++;
            }
        }

        return cardList;
    }

    /**
     * It returns the cardinality list of the dataset for all sequences of length <= 2
     * @param   dataset         the hashmap that contains object array lists (transactions) of itemsets(See SPEck.loadDataset method).
     * @return  cardList   a hashmap whose keys are the unique lengths of the collection of itemsets in the dataset
     *                          and values are lists containing all sequences with that itemlength.
     */
    public static Int2ObjectOpenHashMap<ObjectArrayList<Sequence>> getCardListPairs(
            Dataset dataset){
        Int2ObjectOpenHashMap<ObjectArrayList<Sequence>> cardList = new Int2ObjectOpenHashMap<>();
        int pos = 0;
        //For every pair of itemsets in every transaction, create a sequence object
        //and add it to the cardList with length equal to the itemlength of the sequence
        for (ObjectArrayList<Itemset> trans : dataset) {
            for (int i = 0; i < trans.size(); i++) {
                trans.get(i).setAddress(pos + i); //Set address in itemset object
                for (int j = i; j < trans.size(); j++) {
                    trans.get(j).setAddress(pos+j); //Set address in itemset object
                    if (i == j) { //Same itemset -> sequence of size 1
                        int len = trans.get(i).getLength();
                        if (!(cardList.containsKey(len))) {
                            cardList.put(len, new ObjectArrayList<>());
                        }
                        cardList.get(len).add(new Sequence(trans.get(i), len));
                    } else { //Different itemset -> sequence of size 2
                        int len = trans.get(i).getLength() + trans.get(j).getLength();
                        if (!(cardList.containsKey(len))) {
                            cardList.put(len, new ObjectArrayList<>());
                        }
                        cardList.get(len).add(new Sequence(trans.get(i), trans.get(j), len));
                    }
                }
            }
            pos += trans.size();
        }
        return cardList;
    }

    /**
     * It removes all sequences containing any itemsets in a or b
     * @param   a      the first sequence to be swapped
     * @param b        the second sequence to be swapped
     * @param cardList the hashmap containing all sequences organized into object array lists
     *                 for each itemlength
     * @param datasetRandom the object array list of object array lists (transactions) of itemsets
     */
    public static long removeCardLists(Sequence a, Sequence b,
                                       Int2ObjectOpenHashMap<ObjectArrayList<Sequence>> cardList,
                                       Dataset datasetRandom) {
        long start = System.nanoTime();
        int tidA = a.first.getTid();
        int tidB = b.first.getTid();
        if (tidA == tidB) return System.nanoTime() - start;

        //Create set of ints
        Set<Integer> cardListsToCheck = new HashSet<>();

        //For every pair of itemsets with one from the sequence to be swapped
        //and one from the transaction, add the combined itemlength to the set
        for (Itemset i : a.itemsets) {
            for (Itemset j : datasetRandom.get(tidA)) {
                int len;
                if (i.equals(j)) {
                    len = i.getLength();
                } else {
                    len = i.getLength() + j.getLength();
                }
                cardListsToCheck.add(len);
            }
        }
        for (Itemset i : b.itemsets) {
            for (Itemset j : datasetRandom.get(tidB)) {
                int len;
                if (i.equals(j)) {
                    len = i.getLength();
                } else {
                    len = i.getLength() + j.getLength();
                }
                cardListsToCheck.add(len);
            }
        }

        //Only check cardLists whose length is in the set
        //Ensures that cardLists that don't contain the swapped itemsets aren't checked
        for (int i : cardListsToCheck) {
            cardList.get(i).removeIf(s -> s.overlaps(a) || s.overlaps(b));
        }



        return System.nanoTime() - start;

    }

    /**
     * updates the cardinality list of the dataset
     * @param   a               the first sequence to be swapped.
     * @param   b               the second sequence to be swapped.
     * @param cardList          a hashmap whose keys are the unique lengths of the collection of itemsets in the dataset
     *                          and values are lists containing all sequences with that itemlength.
     * @param datasetRandom     the hashmap that contains object array lists (transactions) of itemsets(See SPEck.loadDataset method).
     */
    public static long updateCardLists(Sequence a, Sequence b,
                                      Int2ObjectOpenHashMap<ObjectArrayList<Sequence>> cardList,
                                       Dataset datasetRandom){
        long start = System.nanoTime();
        int tidA = a.first.getTid();
        int tidB = b.first.getTid();
        if (tidA == tidB) return System.nanoTime() - start;

        //Loop through transaction
        int tidASize = datasetRandom.get(tidA).size();
        for (int i = 0; i < tidASize; i++) {
            Itemset isI = datasetRandom.get(tidA).get(i);
            for (int j = i; j < tidASize; j++) {
                Itemset isJ = datasetRandom.get(tidA).get(j);
                if (a.contains(isI) || a.contains(isJ)){ //If either itemset is in the swapped sequence
                    int len;
                    Sequence seq;
                    if (isI.equals(isJ)){ //If i is the same itemset as j (same address)
                        len = isI.getLength();
                        seq = new Sequence(isI, len);
                    } else {
                        len = isI.getLength() +  isJ.getLength();
                        seq = new Sequence(isI, isJ, len);
                    }
                    if (!(cardList.containsKey(len))) cardList.put(len, new ObjectArrayList<>());
                    cardList.get(len).add(seq); //Create new sequence (above) and add it to card list
                }
            }
        }

        //Same as above loop for sequence B
        int tidBSize = datasetRandom.get(tidB).size();
        for (int i = 0; i < tidBSize; i++) {
            Itemset isI = datasetRandom.get(tidB).get(i);
            for (int j = i; j < tidBSize; j++) {
                Itemset isJ = datasetRandom.get(tidB).get(j);
                if (b.contains(isI) || b.contains(isJ)){
                    int len;
                    Sequence seq;
                    if (isI.equals(isJ)){
                        len = isI.getLength();
                        seq = new Sequence(isI, len);
                    } else {
                        len = isI.getLength() +  isJ.getLength();
                        seq = new Sequence(isI, isJ, len);
                    }
                    if (!(cardList.containsKey(len))) cardList.put(len, new ObjectArrayList<>());
                    cardList.get(len).add(seq);
                }
            }
        }
        return System.nanoTime() - start;
    }


    /**
     * It returns the transaction id of the element in the given position
     * @param   sA   the first sequence to be swapped
     * @param   sB   the second sequence to be swapped
     * @param datasetRandom the hashmap that contains object array lists (transactions) of itemsets(See SPEck.loadDataset method).
     */

    public static void swapSeq(Sequence sA, Sequence sB, Dataset datasetRandom) {
        int tidA = sA.first.getTid();
        int tidB = sB.first.getTid();

        if (tidA == tidB) return;
        //Remove itemsets from old transaction, set new tid, add to new transaction
        for (Itemset i : sA.itemsets) {
            datasetRandom.get(tidA).remove(i);
            i.setTid(tidB);
            datasetRandom.get(tidB).add(i);
        }
        for (Itemset i : sB.itemsets) {
            datasetRandom.get(tidB).remove(i);
            i.setTid(tidA);
            datasetRandom.get(tidA).add(i);
        }
    }

    /**
     * It creates a transactional dataset from a one-dimensional list
     * @param   datasetList   an objectarraylist of itemsets in positions 1 to m
     * @param   datasetRandom    the hashmap that contains object array lists
     *                           (transactions) of itemsets(See SPEck.loadDataset method).
     */
    public static void buildDatasetFromList(ObjectArrayList<Itemset> datasetList,
                                            Dataset datasetRandom){
        int pos = 0;
        for (ObjectArrayList<Itemset> trans : datasetRandom) {
            for (int i = 0; i < trans.size(); i++) {
                trans.set(i, datasetList.get(pos));
                pos++;
            }
        }
    }

    /**
     * Returns an index of the array weights randomly selected with probability relative to their weight
     * @param   weights   an array of weights representing the likelihood of each index getting chosen
     * @return  int       an int from 0 to weights.length - 1
     */

    public static int randomWeightedChoice(double[] weights) {
        double n = Math.random();
        double k = 0;
        for (int i = 0; i < weights.length; i++) {
            k += weights[i];
            if (n <= k) return i;
        }
        return weights.length - 1;
    }

    /**
     * checks whether a potential swap would maintain the original frequencies of both itemsets
     * @param   i1               the first itemset in the potential swap
     * @param   i2               the second itemset in the potential swap
     * @param   tids    the hashmap that contains number of appearances of each itemset in each transaction
     * @return                   boolean determining whether the swap is valid
     */
    public static boolean isValidSwap(Itemset i1, Itemset i2, Int2ObjectOpenHashMap<Int2IntOpenHashMap> tids){

        //get number of times each itemset appears in each transaction
        int sa1 = tids.get(i1.getId()).get(i1.getTid());
        int sa2 = tids.get(i1.getId()).get(i2.getTid());
        int sb1 = tids.get(i2.getId()).get(i1.getTid());
        int sb2 = tids.get(i2.getId()).get(i2.getTid());

        //if the itemset appears only once in t1, it cannot already appear in t2
        //if the itemset appears more than once in t1, it must already appear in t2
        if (sa1 == 1 && sa2 != 0) return false;
        if (sa1 > 1 && sa2 == 0) return false;
        if (sb1 == 1 && sb2 != 0) return false;
        if (sb1 > 1 && sb2 == 0) return false;
        return true;
    }



    /**
     * clones an object such that the new object does not point to the original object using a bit stream
     * @param   obj   the object to be cloned
     * @return  objCopy  a deep copy of obj
     */
    public static Object deepCopy(Object obj) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(obj);
        oos.flush();
        oos.close();
        bos.close();
        byte[] byteData = bos.toByteArray();

        ByteArrayInputStream bais = new ByteArrayInputStream(byteData);
        return new ObjectInputStream(bais).readObject();
    }

    /**
     * returns the support in the random dataset of all the sequences that are frequent in the original dataset
     * @param  dataset         the hashmap that contains object array lists (transactions) of
     *                         itemsets(See SPEck
     *                         .loadDataset method).
     * @param fspSupOrigin      the hashmap for the original dataset where the keys are strings representing sequences
     *                          and the values are their supports for all frequent sequences
     * @return  fspSupRandom  the hashmap for the random dataset where the keys are strings representing sequences
     *      *                          and the values are their supports for all frequent sequences in the original dataset
     */


    /**
    This class is used for representing sequences of itemsets, with first and second denoting their addresses.
     */

    protected static class Sequence implements Serializable {
        public Itemset first;
        public Itemset second;
        public int length;
        public int itemlength;
        public ObjectArrayList<Itemset> itemsets = new ObjectArrayList<>();

        //Constructor if Sequence only has one itemset
        public Sequence(Itemset first, int itemlength){
            this.first = first;
            this.length = 1;
            this.itemlength = itemlength;
            itemsets.add(this.first);
        }
        //Constructor is sequence has two itemsets
        public Sequence(Itemset first, Itemset second, int itemlength){
            if (first.getAddress() < second.getAddress()) {
                this.first = first;
                this.second = second;
            } else {
                this.first = second;
                this.second = first;
            }

            this.length = 2;
            this.itemlength = itemlength;

            itemsets.add(this.first);
            itemsets.add(this.second);
        }
        //whether or not two sequences share any itemsets
        public boolean overlaps(Sequence other){
            for (Itemset i : itemsets) {
                for (Itemset j : other.itemsets) {
                    if (i.equals(j)) return true;
                }
            }
            return false;
        }
        //whether or not sequence contains a given itemset
        public boolean contains(Itemset itemset){
            for (Itemset i : itemsets) {
                if (i.getAddress() == itemset.getAddress()) return true;
            }
            return false;
        }

        //Two Sequences are equal when they have the same length, itemlength, and contain the
        //same itemsets
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Sequence) {
                Sequence s = (Sequence) obj;
                if (!(this.length == s.length && this.itemlength == s.itemlength)) return false;
                for (int i = 0; i < length; i++) {
                    if (!itemsets.get(i).equals(s.itemsets.get(i))) return false;
                }
                return true;
            }
            return false;
        }

        @Override
        public int hashCode() {
            String s = this.first.getId() +"-";
            if (this.second != null) s += this.second.getId();
            s += "-" + this.length+"-"+this.itemlength;
            return s.hashCode();
        }
    }

    //Class used to represent an itemset
    protected static class Itemset implements Serializable {
        private String string;
        private int length;
        private int id;
        private int tid;
        private int address;

        public Itemset(String string, int id, int tid) {
            this.length = string.split(" ").length;
            this.id = id;
            this.string = string;
            this.tid = tid;
        }
        //Constructor only used for tests
        public Itemset(int address) {
            this.address = address;
        }
        //Constructor only used for tests
        public Itemset(int address, int tid, int length) {
            this.address = address;
            this.tid = tid;
            this.length = length;
        }

        public String getString() {
            return string;
        }

        public int getLength() {
            return length;
        }

        public int getId() {
            return id;
        }

        public int getAddress() {
            return address;
        }

        public int getTid() {
            return tid;
        }

        public void setAddress(int address) {
            this.address = address;
        }

        public void setTid(int tid) {
            this.tid = tid;
        }

        public boolean idEquals(Itemset i){
            return i.getId() == this.id;
        }

        //Itemsets are equivalent when addresses are equal
        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Itemset) {
                return ((Itemset) obj).getAddress() == this.getAddress();
            }
            return false;
        }
    }

    /**
     * Private class that implements a simple pair structure where the key and the value
     * are both int
     */
    protected static class Pair implements Serializable{
        int x;
        int y;
        Pair(int x,int y){
            this.x=x;
            this.y=y;
        }

        public boolean equals(Pair other){
            return this.x == other.x && this.y == other.y;
        }
    }

    /**
     * Private class that implements a simple generic pair structure
     */
    protected static class PairT<T1,T2> implements Serializable{
        T1 x;
        T2 y;
        PairT(T1 x,T2 y){
            this.x=x;
            this.y=y;
        }
    }
    
    public static class Dataset extends ObjectArrayList<ObjectArrayList<Itemset>> {
        public Dataset() {

        }
    }
}

