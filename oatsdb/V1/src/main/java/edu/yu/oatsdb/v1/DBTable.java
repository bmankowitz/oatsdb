package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.ClientNotInTxException;
import edu.yu.oatsdb.base.TxStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;

public class DBTable<K, V> implements Serializable, Map<K, V> {
    enum methodType{
        GET, PUT, REMOVE;
    }

    //plan: have a hashmap of <K, HashSet<Thread>>. whenever a thread is waiting, set add it to the hashset
    //when that key is removed, send an interrupt to that thread. YAY JAVA!
    private final HashMap<K, V> official = new HashMap<K, V>(); //The single, primary hashmap
    // FIXME: 7/27/2021 REPLACE HASHSET WITH ConcurrentLinkedQueue for thread safety
    private final HashSet<K> lockedKeys = new HashSet<>();
    private final ThreadLocal<HashMap<K, methodType>> keysInTx = ThreadLocal.withInitial(HashMap::new);
    private final ThreadLocal<HashMap<K, V>> shadow = ThreadLocal.withInitial(HashMap::new);
    protected String tableName;
    protected Class<K> keyClass;
    protected Class<V> valueClass;

    private final static Logger logger = LogManager.getLogger(DBTable.class);


    private V serializeV(V value) {
        //https://howtodoinjava.com/java/serialization/how-to-do-deep-cloning-using-in-memory-serialization-in-java/

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream in = new ObjectInputStream(bis);
            V retVal = (V) in.readObject();
            return retVal;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Failed to serialize");
    }
    protected String getName(){
        return tableName;
    }
    protected void ensureTableIsClean(){
        //starting a tx
        if(!shadow.get().isEmpty() || !keysInTx.get().isEmpty()){
            //if the shadow table is not empty, there is an incomplete Tx somewhere. Or some other error. Either way,
            //this is a catastrophic failure. Abort
            if(shadow.get().isEmpty()) logger.error("EMPTY SHADOW??");
            if(keysInTx.get().isEmpty()) logger.error("EMPTY KEYS??");
            throw new RuntimeException("There is an incomplete Tx somewhere: "
                    + "\nShadowArray:" + shadow.get() + "ShadowisEmpty: " + shadow.get().isEmpty()
                    + "\nkeysInTx: " + keysInTx.get() + "keysIsEmpty: " + keysInTx.get().isEmpty());
        }
    }

    protected void commitCurrentTable(){
        //committing a tx
        unlockAndSetKeys(TxStatus.COMMITTING);
    }

    protected void rollbackCurrentTable(){
        //rolling back a tx
        unlockAndSetKeys(TxStatus.ROLLING_BACK);
    }

    private void addKeyToTx(K key, methodType method){
        //logger.debug(Thread.currentThread() + "locking: " + key);
        System.out.println(Thread.currentThread() + "locking: " + key);
        // FIXME: 7/27/2021 This println somehow saves the day. need to investigate
        lockedKeys.add(key);
        keysInTx.get().put(key,method);
    }
    private void unlockAndSetKeys(TxStatus status){
        //for commit/rollback: remove the lock on these keys
        //for committing:
        for (Map.Entry<K, methodType> entry : keysInTx.get().entrySet()) {
                //logger.debug(Thread.currentThread() + "unlocking: " + entry.getKey());
            synchronized (this) {
                switch (entry.getValue()) {
                    case GET:
                        //do nothing except unlock
                        break;
                    case PUT:
                        //if this is a commit, do the commit
                        if (status == TxStatus.COMMITTING)
                            official.put(entry.getKey(), shadow.get().get(entry.getKey()));
                        break;
                    case REMOVE:
                        //if this is a commit, do the commit
                        if (status == TxStatus.COMMITTING) official.remove(entry.getKey());
                        break;
                }
                lockedKeys.remove(entry.getKey());
            }
                //logger.debug(Thread.currentThread() + "unlocked: " + entry.getKey());
        }

        //now we need to reset shadow db and keysInTx:
        shadow.remove();
        //keysInTx:
        keysInTx.remove();


    }
    private void lockedKeyWait(K key){
        //logger.debug("Waiting for key to be unlocked: " + key);
//        System.out.println("Current locked keys " + lockedKeys);
//        System.out.println(Thread.currentThread() + "Current table: " + shadow.get());
//        System.out.println(Thread.currentThread() + "Waiting for key to be unlocked: " + key);
        long startTime = System.currentTimeMillis();
        while(lockedKeys.contains(key)){
            /* Spin lock :( */
            if((System.currentTimeMillis() - startTime) >= Globals.lockingTimeout){
                //logger.error(Thread.currentThread() + ": FAILED - Timeout");
                //we exceeded the timeout. Need to roll back:
                //TODO: Find a more elegant way to do this than copy paste:
                Thread currentThread = Thread.currentThread();
                TxImpl tx = Globals.threadTxMap.get(currentThread);
                if(!Globals.threadTxMap.containsKey(currentThread)) {
                    throw new IllegalStateException("This thread is not in a transaction");
                }
                tx.setStatus(TxStatus.ROLLING_BACK);
                Globals.threadTxMap.replace(currentThread, tx);
                Globals.revertThreadTables(currentThread);
                tx.setStatus(TxStatus.ROLLEDBACK);
                Globals.threadTxMap.replace(currentThread, tx);
                Globals.threadTxMap.remove(currentThread);


                //logger.error(Thread.currentThread() + ": FAILED - Finished Rollback. Throwing Exception");
                throw new edu.yu.oatsdb.base.ClientTxRolledBackException("timeout exceeded");
            }
        }
        //logger.debug("Key Unlocked!: " + key);
        //need to update shadow value from official:
        V newValue = official.get(key);
        shadow.get().put(key, newValue);
    }

    @SuppressWarnings("unchecked")
    public V get(Object key) {
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("Not in Tx");
        //Now this is a valid thread. In case it was not added directly, add it now:
        Globals.addTableToThread(this.getName(), Thread.currentThread());
        //if the key is locked and not by this transaction
        if(lockedKeys.contains(key) && !keysInTx.get().containsKey(key)){
            lockedKeyWait((K) key);
        }
        //if the key is already in this transaction (ie it is cached in the shadow map)
        else if(keysInTx.get().containsKey(key)){
            return serializeV(shadow.get().get(key));
        }
        //if this key is not in a transaction (new use - need to add to shadow cache (like a cache miss))
        else if(!keysInTx.get().containsKey(key)){
            //we need to update this key in the shadowDB:
//            HashMap<K, V> tempShadow = shadow.get();
//            tempShadow.put((K) key, official.get(key));
//            shadow.set(tempShadow);
            addKeyToTx((K) key, methodType.GET);
            shadow.get().put((K) key, official.get(key));
            return serializeV(shadow.get().get(key));
        }

        return serializeV(shadow.get().get(key));
    }

    public V put(K key, V value) {
        V retVal;
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("This thread" +
                "is not currently in a transaction" + Thread.currentThread());
        if(key == null) throw new IllegalArgumentException("Null is not a valid key");
        //Now this is a valid thread. In case it was not added directly, add it now:
        Globals.addTableToThread(this.getName(), Thread.currentThread());
        //if the key is locked and not by this transaction
        if(lockedKeys.contains(key) && !keysInTx.get().containsKey(key)){
            lockedKeyWait((K) key);
        }
        //now that the previously locked key is available and loaded with official value, update it;
        addKeyToTx((K) key, methodType.PUT);
        retVal = shadow.get().put(key, serializeV(value));
        return serializeV(retVal);
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        V retVal;
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("Not in Tx");
        //Now this is a valid thread. In case it was not added directly, add it now:
        Globals.addTableToThread(this.getName(), Thread.currentThread());
        if(lockedKeys.contains(key) && !keysInTx.get().containsKey(key)){
            lockedKeyWait((K) key);
        }
        addKeyToTx((K) key, methodType.REMOVE);
//        HashMap<K, V> tempShadow = shadow.get();
//        retVal= tempShadow.remove(key);
//        shadow.set(tempShadow);
        retVal = shadow.get().remove(key);
        return serializeV(retVal);
    }

    //if there are uncommitted changes
    protected boolean isDirty(){
        // do the compare separately to avoid NPE

        return !shadow.get().isEmpty() || !keysInTx.get().isEmpty();

//        if(shadow.get() != null || keysInTx.get() != null) return false;
//        else return (!shadow.get().isEmpty());
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        if(shadow.get().isEmpty()) return "[]";
        for(K key: shadow.get().keySet()){
            sb.append("[").append(key).append(" --> ").append(shadow.get().get(key)).append("]\n");
        }
        for(K key: official.keySet()){
            sb.append("OFFICIAL: [").append(key).append(" --> ").append(shadow.get().get(key)).append("]\n");
        }
        return sb.toString();
    }
    public int size() {
        throw new RuntimeException("YouCan'tHaveItException");
    }
    public boolean isEmpty() {
        throw new RuntimeException("YouCan'tHaveItException");
    }
    public boolean containsKey(Object key) {
        throw new RuntimeException("YouCan'tHaveItException");
    }
    public boolean containsValue(Object value) {
        throw new RuntimeException("YouCan'tHaveItException");
    }
    public void putAll(Map<? extends K, ? extends V> m) {
        throw new RuntimeException("YouCan'tHaveItException");
    }
    public void clear() {
        throw new RuntimeException("YouCan'tHaveItException");
    }
    public Set<K> keySet() {
        throw new RuntimeException("YouCan'tHaveItException");
    }
    public Collection<V> values() {
        throw new RuntimeException("YouCan'tHaveItException");
    }
    public Set<Entry<K, V>> entrySet() {
        throw new RuntimeException("YouCan'tHaveItException");
    }
}
