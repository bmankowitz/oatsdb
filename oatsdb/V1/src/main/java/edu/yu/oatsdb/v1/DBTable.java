package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.ClientNotInTxException;
import edu.yu.oatsdb.base.TxStatus;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

public class DBTable<K, V> implements Serializable, Map<K, V> {
    enum methodType{
        GET, PUT, REMOVE;
    }

    //plan: have a hashmap of <K, HashSet<Thread>>. whenever a thread is waiting, set add it to the hashset
    //when that key is removed, send an interrupt to that thread. YAY JAVA!
    private volatile HashMap<K, V> official = new HashMap<K, V>(); //The single, primary hashmap
    private volatile HashSet<K> lockedKeys = new HashSet<>();
    private final ThreadLocal<HashMap<K, methodType>> keysInTx = ThreadLocal.withInitial(() -> new HashMap<>());
    private final ThreadLocal<HashMap<K, V>> shadow = ThreadLocal.withInitial(() -> new HashMap<>());
    @Deprecated
    private final ThreadLocal<String> error = new ThreadLocal<String>();
    private String tableName;
    private TxStatus status;
    protected Class<K> keyClass;
    protected Class<V> valueClass;
    protected HashMap<K, V> getOfficial(){
        return official;
    }

    protected void setName(String name){
        tableName = name;
    }
    protected String getName(){
        return tableName;
    }
    protected synchronized void startTx(){
        //starting a tx
        if(!shadow.get().isEmpty() || !keysInTx.get().isEmpty()){
            error.set("There is an incomplete Tx somewhere: "
                    + "\nShadowArray:" + shadow.get() + "ShadowisEmpty: " + shadow.get().isEmpty()
                    + "\nkeysInTx: " + keysInTx + "keysIsEmpty: " + keysInTx.get().isEmpty());
            if(shadow.get().isEmpty()) System.out.println("EMPTY SHADOW??");
            if(keysInTx.get().isEmpty()) System.out.println("EMPTY KEYS??");

            //if the shadow table is not empty, there is an incomplete Tx somewhere. Or some error
            throw new RuntimeException(error.get());
            //TODO: should we update the shadow db?
        }
        shadow.set(new HashMap<>(official));
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
        System.out.println(Thread.currentThread() + "locking: " + key);
        HashMap<K, methodType> tempKeys = keysInTx.get();
        tempKeys.put(key, method);
        keysInTx.set(tempKeys);
        lockedKeys.add(key);
    }
    private synchronized void unlockAndSetKeys(TxStatus status){
        //for commit/rollback: remove the lock on these keys
        //for committing:
        if(status == TxStatus.COMMITTING) {
            for (Map.Entry<K, methodType> entry : keysInTx.get().entrySet()) {
                switch (entry.getValue()) {
                    case GET:
                        //do nothing except unlock
                        System.out.println(Thread.currentThread() + "unlocking: " + entry.getKey());
                        lockedKeys.remove(entry.getKey());
                        System.out.println(Thread.currentThread() + "unlocked: " + entry.getKey());
                        break;
                    case PUT:
                        lockedKeys.remove(entry.getKey());
                        official.put(entry.getKey(), shadow.get().get(entry.getKey()));
                        System.out.println(Thread.currentThread() + "unlocking: " + entry.getKey());
                        lockedKeys.remove(entry.getKey());
                        System.out.println("unlocked: " + entry.getKey());
                        break;
                    case REMOVE:
                        lockedKeys.remove(entry.getKey());
                        official.remove(entry.getKey());
                        System.out.println(Thread.currentThread() + "unlocking: " + entry.getKey());
                        lockedKeys.remove(entry.getKey());
                        System.out.println("unlocked: " + entry.getKey());
                        break;
                }
            }
        }
        //error with these keys. unlock and dump them
        //TODO: isn't this for a Rollback?
        else{
            for(K key : keysInTx.get().keySet()){
                lockedKeys.remove(key);
                System.out.println(Thread.currentThread() + "unlocking: " + key);
                lockedKeys.remove(key);
                System.out.println("unlocked: " + key);
            }
        }
        //now we need to reset shadow db and keysInTx:
        HashMap<K, V> tempMap = shadow.get();
        tempMap.clear();
        shadow.set(tempMap);
        shadow.remove();
        //keysInTx:
        HashMap<K, methodType> tempKeys = keysInTx.get();
        tempKeys.clear();
        keysInTx.set(tempKeys);
        keysInTx.remove();


    }
    private void keyLocked(K key){
        System.out.println("Waiting for key to be unlocked: " + key);

        long startTime = System.currentTimeMillis();
        while(lockedKeys.contains(key)){
            /* Spin lock :( */
            if((System.currentTimeMillis() - startTime) >= Globals.lockingTimeout){
                //The thread timed out.
                System.out.println(Thread.currentThread() + ": FAILED - Timeout");
                //we exceeded the timeout. Need to roll back:
                //TODO: Find a more elegant way to do this than copy paste:
                Thread t = Thread.currentThread();
                TxImpl tx = Globals.threadTxMap.get(t);
                if(!Globals.threadTxMap.containsKey(t)) {
                    //if the thread is not already in the thread table
                    throw new IllegalStateException("This thread is not in a transaction");
                }
                tx.setStatus(TxStatus.ROLLING_BACK);
                Globals.threadTxMap.replace(t, tx);
                //after successfully transacting the transaction, allow this thread to add a new tx
                Globals.revertAll();
                tx = Globals.threadTxMap.get(t);
                tx.setStatus(TxStatus.ROLLEDBACK);
                Globals.threadTxMap.replace(t, tx);
                Globals.threadTxMap.remove(t);
                System.out.println(Thread.currentThread() + ": FAILED - Finished Rollback. Throwing Exception");
                throw new edu.yu.oatsdb.base.ClientTxRolledBackException("timeout exceeded");
            }
        }
        System.out.println("Key Unlocked!: " + key);
        //need to update shadow value from official:
        V newValue = official.get(key);
        HashMap<K, V> tempShadow = shadow.get();
        tempShadow.put(key, newValue);
        shadow.set(tempShadow);


    }

    @SuppressWarnings("unchecked")
    public V get(Object key) {
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("Not in Tx");
        if(lockedKeys.contains(key) && !keysInTx.get().containsKey(key)){
            keyLocked((K) key);
        }
        else if(keysInTx.get().containsKey(key)){
            return shadow.get().get(key);
        }
        else if(!keysInTx.get().containsKey(key)){
            //we need to update this key in the shadowDB:
            HashMap<K, V> tempShadow = shadow.get();
            tempShadow.put((K) key, official.get(key));
            shadow.set(tempShadow);
            return shadow.get().get(key);
        }
        addKeyToTx((K) key, methodType.GET);
        return shadow.get().get(key);
    }

    public V put(K key, V value) {
        V retVal;
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("This thread" +
                "is not currently in a transaction" + Thread.currentThread());
        if(key == null) throw new IllegalArgumentException("Null is not a valid key");
        if(lockedKeys.contains(key) && !keysInTx.get().containsKey(key)){
            keyLocked((K) key);
        }
        //System.out.println("Putting in a new key: "+key+" from "+Thread.currentThread());
        addKeyToTx((K) key, methodType.PUT);
        HashMap<K, V> tempMap = shadow.get();
        retVal = tempMap.put(key, value);
        shadow.set(tempMap);
        return retVal;
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key) {
        V retVal;
        //TODO: implement appropriate safety checks (ex. current != null, etc)
        if(Globals.threadTxMap.get(Thread.currentThread()) == null) throw new ClientNotInTxException("Not in Tx");
        if(lockedKeys.contains(key) && !keysInTx.get().containsKey(key)){
            keyLocked((K) key);
        }
        addKeyToTx((K) key, methodType.REMOVE);
        HashMap<K, V> tempMap = shadow.get();
        retVal= tempMap.remove(key);
        shadow.set(tempMap);
        return retVal;
    }

    //if there are uncommitted changes
    protected boolean isDirty(){
        if(shadow.get() == null) return false;
        else return !shadow.get().isEmpty();
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
