package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.ClientNotInTxException;
import edu.yu.oatsdb.base.ClientTxRolledBackException;
import edu.yu.oatsdb.base.TxStatus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class DBTable<K, V> implements Serializable, Map<K, V> {
    public DBTable(String tableName, Class<K> keyClass, Class<V> valueClass) {
        this.tableName = tableName;
        this.keyClass = keyClass;
        this.valueClass = valueClass;
    }

    enum methodType{
        GET, PUT, REMOVE
    }


    private final ConcurrentHashMap<K, V> official = new ConcurrentHashMap<>(); //The single, primary hashmap
    // FIXME: 7/27/2021 REPLACE HASHSET WITH ConcurrentLinkedQueue for thread safety
    private final ConcurrentHashMap<K, ReentrantLock> lockedKeys = new ConcurrentHashMap<>();
    private final ThreadLocal<HashMap<K, methodType>> keysInTx = ThreadLocal.withInitial(HashMap::new);
    private final ThreadLocal<HashMap<K, V>> shadow = ThreadLocal.withInitial(HashMap::new);
    protected final AtomicBoolean readyForUse = new AtomicBoolean(false);
    protected final String tableName;
    protected final Class<K> keyClass;
    protected final Class<V> valueClass;
    private final static Logger logger = LogManager.getLogger(DBTable.class);


    @SuppressWarnings("unchecked")
    private V serializeV(V value) {
        //https://howtodoinjava.com/java/serialization/how-to-do-deep-cloning-using-in-memory-serialization-in-java/

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(value);
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream in = new ObjectInputStream(bis);
            return (V) in.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        throw new RuntimeException("Failed to serialize");
    }

    @SuppressWarnings("unchecked")
    private K serializeK(K key) {
        //https://howtodoinjava.com/java/serialization/how-to-do-deep-cloning-using-in-memory-serialization-in-java/

        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(key);
            ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
            ObjectInputStream in = new ObjectInputStream(bis);
            return (K) in.readObject();
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
            if(shadow.get().isEmpty()) //if (Globals.log) if (Globals.log) logger.error("EMPTY SHADOW??");
            if(keysInTx.get().isEmpty()) if (Globals.log) logger.error("EMPTY KEYS??");
            throw new RuntimeException("There is an incomplete Tx somewhere: "
                    + "\nShadowArray:" + shadow.get() + "ShadowIsEmpty: " + shadow.get().isEmpty()
                    + "\nkeysInTx: " + keysInTx.get() + "keysIsEmpty: " + keysInTx.get().isEmpty());
        }
        readyForUse.set(true);
    }

    protected void commitCurrentTable(){
        //committing a tx
        unlockAndSetKeys(TxStatus.COMMITTING);
    }

    protected void rollbackCurrentTable(){
        //rolling back a tx
        unlockAndSetKeys(TxStatus.ROLLING_BACK);
    }

    private void initializeAndLockKeyToTx(K key, methodType method){
        if(keysInTx.get().containsKey(key)){
            if (Globals.log) logger.debug("Skipping key {} with shadow value {}  and official value {} via {}",
                    key, shadow.get().get(key), official.get(key), method);
            return;
        }
        if (Globals.log) logger.debug("Locking key {} with shadow value {}  and official value {} via {}",
                key, shadow.get().get(key), official.get(key), method);
        if (Globals.log) logger.debug("\n Shadow: {} \n Official: {} \n keysInTX: {} \n lockedKeys: {}",
                shadow.get(), official, keysInTx.get(), lockedKeys);
        // FIXME: 7/27/2021 This println somehow saves the day. need to investigate
        key = serializeK(key);
        if(lockedKeys.containsKey(key) && !lockedKeys.get(key).isLocked()){
            if (Globals.log) logger.debug("Lock {} for key {} already exists. Cont.", lockedKeys.get(key), key);
            return;
        }
        lockedKeys.put(key, new ReentrantLock(true));
        lockedKeys.get(key).lock();
        if (Globals.log) logger.debug("Lock {} for key {}", lockedKeys.get(key), key);

        keysInTx.get().put(key,method);
        //update it with the official value. NOTE that this will be overwritten by put and remove, but it is needed for
        //get, and to ensure the remove method returns the correct value
        shadow.get().put(key, official.get(key));
        if (Globals.log) logger.debug("Updated shadow value to the official value {} for key {}", shadow.get().get(key), key);

    }
    private void unlockAndSetKeys(TxStatus status){
        //for commit/rollback: remove the lock on these keys
        //for committing:
        for (Map.Entry<K, methodType> entry : keysInTx.get().entrySet()) {
            if (Globals.log) logger.debug("Unlocking and setting {} -> {} via {}", entry.getKey(), shadow.get().get(entry.getKey()), entry.getValue());

            if (status == TxStatus.COMMITTING) {
                //update the official value with the latest one from the shadow table
                if (official.get(entry.getKey()) == null ||
                        !official.get(entry.getKey()).equals(shadow.get().get(entry.getKey()))) {
                    if (shadow.get().get(entry.getKey()) == null) {
                        official.remove(entry.getKey());
                    } else official.put(entry.getKey(), shadow.get().get(entry.getKey()));
                }
            }
            //quick hack:
            while(lockedKeys.get(entry.getKey()).getHoldCount() > 0)
                lockedKeys.get(entry.getKey()).unlock();
            //lockedKeys.get(entry.getKey()).unlock();

            if (Globals.log) logger.debug("Finished unlocking and  setting {} -> {} via {} (Official)", entry.getKey(), official.get(entry.getKey()), entry.getValue());
        }

        //now we need to reset shadow db and keysInTx:
        shadow.remove();
        //keysInTx:
        keysInTx.remove();

        readyForUse.set(true);


    }
    private void waitAndUpdateToOfficialIfLocked(K key) {
        if(lockedKeys.get(key).isHeldByCurrentThread()){
            if (Globals.log) logger.debug("Key {} was locked by the current thread: {}",key, lockedKeys.get(key));
            return;
        }
        if (Globals.log) logger.debug("Waiting for key to be unlocked: " + key);
        try {
            if (lockedKeys.get(key).tryLock(Globals.lockingTimeout, TimeUnit.MILLISECONDS)) {
                lockedKeys.get(key).unlock();
                if (Globals.log) logger.debug("Key {} Unlocked: {}",key, lockedKeys.get(key));
                //need to update shadow value from official:
                V newValue = official.get(key);
                shadow.get().put(key, newValue);
            } else {
                Globals.revertThreadTables();
                throw new ClientTxRolledBackException("The transaction was unable to be processed due to locked keys");
            }
        } catch (InterruptedException e) {
            if(lockedKeys.get(key).isHeldByCurrentThread()) {
                if (Globals.log) logger.debug("Key Unlocked!: " + key);
                //need to update shadow value from official:
                V newValue = official.get(key);
                shadow.get().put(key, newValue);
            }
            else throw new RuntimeException("idk what");
        }
//        while(lockedKeys.get(key) != null && lockedKeys.get(key).isLocked()){
//            /* Spin lock :( */
//            if((System.currentTimeMillis() - startTime) >= Globals.lockingTimeout){
//                if (Globals.log) logger.error(Thread.currentThread() + ": FAILED - Timeout");
//                //we exceeded the timeout. Need to roll back:
//                //TODO: Find a more elegant way to do this than copy paste:
//
//                TxImpl tx = Globals.threadTxMap.get(Thread.currentThread());
//                if(!Globals.threadTxMap.containsKey(Thread.currentThread())) {
//                    throw new IllegalStateException("This thread is not in a transaction");
//                }
//                tx.setStatus(TxStatus.ROLLING_BACK);
//                Globals.threadTxMap.replace(Thread.currentThread(), tx);
//                Globals.revertThreadTables();
//                tx.setStatus(TxStatus.ROLLEDBACK);
//                Globals.threadTxMap.replace(Thread.currentThread(), tx);
//                Globals.threadTxMap.remove(Thread.currentThread());
//                if (Globals.log) logger.error(Thread.currentThread() + ": FAILED - Finished Rollback. Throwing Exception");
//                //FIXME: REMOVE THIS !!!!!
//
//                //Runtime.getRuntime().halt(333);
//                throw new edu.yu.oatsdb.base.ClientTxRolledBackException("timeout exceeded");
//            }
//        }

    }

    @SuppressWarnings("unchecked")
    public V get(Object key) {
        long startTime = System.currentTimeMillis();
        while(!readyForUse.get()) {
            /* Spin lock :( */
            if ((System.currentTimeMillis() - startTime) >= Globals.lockingTimeout) {
                throw new RuntimeException("Tried to perform operation before table was ready");
            }
        }
        if (Globals.log) logger.debug("Received GET for key {}", key);

        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("Not in Tx");
        //Now this is a valid thread. In case it was not added directly, add it now:
        Globals.addTableToThread(this.getName());
        initializeAndLockKeyToTx((K) key, methodType.GET);
        waitAndUpdateToOfficialIfLocked((K) key);

        return serializeV(shadow.get().get(key));
    }

    public V put(K key, V value) {
        long startTime = System.currentTimeMillis();
        while(!readyForUse.get()) {
            /* Spin lock :( */
            if ((System.currentTimeMillis() - startTime) >= Globals.lockingTimeout) {
                throw new RuntimeException("Tried to perform operation before table was ready");
            }
        }
        if (Globals.log) logger.debug("Received PUT for key {} to value {}", key, value);

        V retVal;
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("This thread" +
                "is not currently in a transaction" + Thread.currentThread());
        if(key == null) throw new IllegalArgumentException("Null is not a valid key");
        //Now this is a valid thread. Lock the key. In case it was not added directly, add it now:
        Globals.addTableToThread(this.getName());
        initializeAndLockKeyToTx((K) key, methodType.PUT);
        waitAndUpdateToOfficialIfLocked((K) key);
        retVal = shadow.get().put(key, serializeV(value));
        if (Globals.log) logger.debug("Done PUTTING key {} with shadow value {}  and official value {}",
                key, shadow.get().get(key), official.get(key));
        return serializeV(retVal);
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        long startTime = System.currentTimeMillis();
        while(!readyForUse.get()) {
            /* Spin lock :( */
            if ((System.currentTimeMillis() - startTime) >= Globals.lockingTimeout) {
                throw new RuntimeException("Tried to perform operation before table was ready");
            }
        }
        if (Globals.log) logger.debug("Received REMOVE for key {}", key);

        V retVal;
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("Not in Tx");
        //Now this is a valid thread. Lock the key. In case it was not added directly, add it now:
        Globals.addTableToThread(this.getName());
        initializeAndLockKeyToTx((K) key, methodType.REMOVE);
        waitAndUpdateToOfficialIfLocked((K) key);
        retVal = shadow.get().remove(key);
        if (Globals.log) logger.debug("Done REMOVING key {} with shadow value {}  and official value {}",
                key, shadow.get().get((K) key), official.get((K) key));
        return serializeV(retVal);
    }

    //if there are uncommitted changes
    protected boolean isDirty(){
        return !shadow.get().isEmpty() || !keysInTx.get().isEmpty();
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
