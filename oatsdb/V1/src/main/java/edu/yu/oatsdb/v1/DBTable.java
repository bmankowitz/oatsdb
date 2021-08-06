package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.ClientNotInTxException;
import edu.yu.oatsdb.base.ClientTxRolledBackException;
import edu.yu.oatsdb.base.SystemException;
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
    private final ConcurrentHashMap<Thread, HashSet<K>> keysInTx = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Thread, HashMap<K,V>> shadow = new ConcurrentHashMap<>();
//    private volatile ThreadLocal<HashSet<K>> keysInTx = ThreadLocal.withInitial(HashSet::new);
//    private volatile ThreadLocal<HashMap<K, V>> shadow = ThreadLocal.withInitial(HashMap::new);
    protected final AtomicBoolean readyForUse = new AtomicBoolean(false);
    public Set<K> exists = ConcurrentHashMap.newKeySet();
    protected final String tableName;
    protected final Class<K> keyClass;
    protected final Class<V> valueClass;
    private final static Logger logger = LogManager.getLogger(DBTable.class);
    //THE PROBLEM IS THE SAME THREAD USING MULTIPLE MAPS!!!!
    //THREADLOCAL IS SHARED BETWEEN THE DIFFERENT MAPS, SO THE KEYS ARE RESET WHEN SWITCHING MAPS


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
        if(!(shadow.get(Thread.currentThread()) == null) || !(keysInTx.get(Thread.currentThread()) == null)){
            //if the shadow table is not empty, there is an incomplete Tx somewhere. Or some other error. Either way,
            //this is a catastrophic failure. Abort
            if(shadow.get(Thread.currentThread()).isEmpty()) //if (Globals.log) if (Globals.log) logger.error("EMPTY SHADOW??");
            if(keysInTx.get(Thread.currentThread()).isEmpty()) if (Globals.log) logger.error("EMPTY KEYS??");
            throw new RuntimeException("There is an incomplete Tx somewhere: "
                    + "\nShadowArray:" + shadow.get(Thread.currentThread()) + "ShadowIsEmpty: " + shadow.get(Thread.currentThread()).isEmpty()
                    + "\nkeysInTx: " + keysInTx.get(Thread.currentThread()) + "keysIsEmpty: " + keysInTx.get(Thread.currentThread()).isEmpty());
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

    private void unlockAndSetKeys(TxStatus status){
        //for commit/rollback: remove the lock on these keys
        //for committing:
        if(keysInTx.get(Thread.currentThread()) == null) return;
        for (K key : keysInTx.get(Thread.currentThread())) {
            if (Globals.log) logger.debug("Unlocking and setting {} -> {}", key,
                    shadow.get(Thread.currentThread()).get(key));

            if (status == TxStatus.COMMITTING) {
                //update the official value with the latest one from the shadow table
                if (official.get(key) == null ||
                        !official.get(key).equals(shadow.get(Thread.currentThread()).get(key))) {
                    if (shadow.get(Thread.currentThread()).get(key) == null) {
                        official.remove(key);
                    } else official.put(key, shadow.get(Thread.currentThread()).get(key));
                }
            }
            //quick hack:
            while(lockedKeys.get(key).getHoldCount() > 0)
                lockedKeys.get(key).unlock();
            exists.remove(key);

            if (Globals.log) logger.debug("Finished unlocking and  setting {} -> {} (Official)",
                    key, official.get(key));
        }

        //now we need to reset shadow db and keysInTx:
        shadow.remove(Thread.currentThread());
        //keysInTx:
        keysInTx.remove(Thread.currentThread());

        readyForUse.set(true);


    }

    public V getValue(final K key, final V value, final methodType method){
        //initializeAndLockKeyToTx(key, method);
        //--------------------------------------
        //if this key does not exist in the lockedKeys, we should probably create it.
        if(!lockedKeys.containsKey(key)){
            lockedKeys.put(key, new ReentrantLock(true));
            logger.debug("Adding new key {} with lock {}" , key, lockedKeys.get(key));

        }
        else{
            logger.debug("Key {} exists", key);
        }
        //now this key definitely exists in the transaction and has a lock which may or may not be activated. Try to
        // lock

        try {
            if(lockedKeys.get(key).tryLock(Globals.lockingTimeout, TimeUnit.MILLISECONDS)){
                //we have the lock
                //need to initialize shadow!
                shadow.computeIfAbsent(Thread.currentThread(), k -> new HashMap<>());

                if (Globals.log) logger.debug("Acquired lock {} for key {} with shadow {} & official {}",
                        lockedKeys.get(key),key, shadow.get(Thread.currentThread()).get(key), official.get(key));
                //if this is the first time seeing this key, we need to load the default original value into shadow tbl
                keysInTx.compute(Thread.currentThread(), (txKey, txValue) -> {
                    if( txValue == null) txValue = new HashSet<>();
                    if(!txValue.contains(key)){
                        //new key:
                        shadow.get(txKey).put(key, official.get(key));
                        txValue.add(key);
                    }
                    //in any case, set value:
                    return txValue;
                });
//                if(!keysInTx.get(Thread.currentThread()).contains(key)){
//                    exists.add(key);
//                    keysInTx.get(Thread.currentThread()).add(key);
//                    shadow.get(Thread.currentThread()).put(key, official.get(key));
//                    logger.debug("Initializing new key {} with SHADOW: {} OFFICIAL: {}" , key, shadow.get(Thread.currentThread()).get(key),
//                            official.get(key));
//                }
                logger.debug("existing keys after insertion: {}, lockcount: {} " , keysInTx.get(Thread.currentThread()),
                        lockedKeys.get(key).getHoldCount());
                //now this key has an initial value and is owned by this thread. return the result
                if (Globals.log) logger.debug("Updating key {} with SHADOW: {} OFFICIAL: {} via {}",
                        key, shadow.get(Thread.currentThread()).get(key), official.get(key), method);
                if(method == methodType.GET) return shadow.get(Thread.currentThread()).get(key);
                if(method == methodType.PUT) return shadow.get(Thread.currentThread()).put(key, value);
                if(method == methodType.REMOVE) return shadow.get(Thread.currentThread()).remove(key);
            }
            else{
                //we were unable to acquire the lock in the given time
                if (Globals.log) logger.debug("Unable to get lock for key {} with shadow {} & official {} lock {}",
                        key, shadow.get(Thread.currentThread()).get(key), official.get(key), lockedKeys.get(key));
                Globals.revertThreadTables();
                throw new ClientTxRolledBackException("Unable to acquire lock on key '"+key+"' within time limit (ms):"
                        + Globals.lockingTimeout);

            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public V get(final Object key) {
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
        final V retVal = serializeV(getValue(serializeK((K) key), null, methodType.GET));
        if (Globals.log) logger.debug("Done GETTING key {} with shadow value {}  and official value {}",
                key, shadow.get(Thread.currentThread()).get(key), official.get(key));
        return retVal;
    }

    public V put(final K key, final V value) {
        long startTime = System.currentTimeMillis();
        while(!readyForUse.get()) {
            /* Spin lock :( */
            if ((System.currentTimeMillis() - startTime) >= Globals.lockingTimeout) {
                throw new RuntimeException("Tried to perform operation before table was ready");
            }
        }
        if (Globals.log) logger.debug("Received PUT for key {} to value {}", key, value);

        final V retVal;
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("This thread" +
                "is not currently in a transaction" + Thread.currentThread());
        if(key == null) throw new IllegalArgumentException("Null is not a valid key");
        //Now this is a valid thread. Lock the key. In case it was not added directly, add it now:
        Globals.addTableToThread(this.getName());
        retVal = serializeV(getValue(serializeK(key), serializeV(value), methodType.PUT));
        if (Globals.log) logger.debug("Done PUTTING key {} with shadow value {}  and official value {}",
                key, shadow.get(Thread.currentThread()).get(key), official.get(key));
        return retVal;
    }

    @SuppressWarnings("unchecked")
    public V remove(final Object key) {
        long startTime = System.currentTimeMillis();
        while(!readyForUse.get()) {
            /* Spin lock :( */
            if ((System.currentTimeMillis() - startTime) >= Globals.lockingTimeout) {
                throw new RuntimeException("Tried to perform operation before table was ready");
            }
        }
        if (Globals.log) logger.debug("Received REMOVE for key {}", key);

        final V retVal;
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("Not in Tx");
        //Now this is a valid thread. Lock the key. In case it was not added directly, add it now:
        Globals.addTableToThread(this.getName());
        retVal = serializeV(getValue(serializeK( (K) key), null, methodType.REMOVE));
        if (Globals.log) logger.debug("Done REMOVING key {} with shadow value {}  and official value {}",
                key, shadow.get(Thread.currentThread()).get((K) key), official.get((K) key));
        return serializeV(retVal);
    }

    //if there are uncommitted changes
    protected boolean isDirty(){
        if(shadow.get(Thread.currentThread()) == null && keysInTx.get(Thread.currentThread()) == null) {
            return false;
        }
        return !shadow.get(Thread.currentThread()).isEmpty() || !keysInTx.get(Thread.currentThread()).isEmpty();
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        if(shadow.get(Thread.currentThread()).isEmpty()) return "[]";
        for(K key: shadow.get(Thread.currentThread()).keySet()){
            sb.append("[").append(key).append(" --> ").append(shadow.get(Thread.currentThread()).get(key)).append("]\n");
        }
        for(K key: official.keySet()){
            sb.append("OFFICIAL: [").append(key).append(" --> ").append(shadow.get(Thread.currentThread()).get(key)).append("]\n");
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
