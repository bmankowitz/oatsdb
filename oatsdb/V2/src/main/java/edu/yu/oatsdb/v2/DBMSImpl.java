package edu.yu.oatsdb.v2;

import edu.yu.oatsdb.base.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.Map;
import java.util.NoSuchElementException;

public enum DBMSImpl implements DBMS, ConfigurableDBMS, ConfigurablePersistentDBMS {
    Instance;
    DBMSImpl(){
        Logger logger = LogManager.getLogger(DBMSImpl.class);
        if(Globals.log) logger.info("Starting to read stored data");
        Globals.readStorage();
        if(Globals.log) logger.info("Finished reading storage");
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(String name, Class<K> keyClass, Class<V> valueClass) {
        //ensure client is in a transaction
        if (Globals.threadTxMap.get(Thread.currentThread()) == null ||
                Globals.threadTxMap.get(Thread.currentThread()).status != TxStatus.ACTIVE) {
            throw new ClientNotInTxException("Unable to get map: not in an active transaction");
        }

        //ensure name is not blank/whitespace:
        if(name.trim().isEmpty()){
            throw new IllegalArgumentException("Invalid map name");
        }
        //ensure keyClass/valueClass is not null:
        if(keyClass == null || valueClass == null){
            throw new IllegalArgumentException("Key/value class cannot be null");
        }
        //ensure this name exists
        if(!Globals.alreadyExists(name)){
            throw new NoSuchElementException("Unable to locate map with name: "+ name);
        }

        //ensure classes match
        DBTable<K, V> table = Globals.getTable(name);
        if(keyClass != table.keyClass || valueClass != table.valueClass){
            throw new ClassCastException("Tried to get map with different classes than on file");
        }
        //ensure serializable
        if(!Serializable.class.isAssignableFrom(keyClass))
            throw new IllegalArgumentException("Key class is not serializable");

        if(!Serializable.class.isAssignableFrom(valueClass))
            throw new IllegalArgumentException("Value class is not serializable");


        Globals.addTableToThread(name);
        return Globals.getTable(name);
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> createMap(String name, Class<K> keyClass, Class<V> valueClass) {


        //ensure this is not for a rolledback tx:
        //ensure we are in a Tx:

        if (Globals.threadTxMap.get(Thread.currentThread()) == null ||
                Globals.threadTxMap.get(Thread.currentThread()).status != TxStatus.ACTIVE) {
            throw new ClientNotInTxException("Cannot create map: not in an active transaction");
        }
        //ensure name is not blank/whitespace:
        if(name.trim().isEmpty()){
            throw new IllegalArgumentException("Invalid map name");
        }
        //ensure keyClass/valueClass is not null:
        if(keyClass == null || valueClass == null){
            throw new IllegalArgumentException("Key/value class cannot be null");
        }
        //ensure serializable
        if(!Serializable.class.isAssignableFrom(keyClass))
            throw new IllegalArgumentException("Key class is not serializable");

        if(!Serializable.class.isAssignableFrom(valueClass))
            throw new IllegalArgumentException("Value class is not serializable");

        //ensure that the name is not in use:
        if(Globals.alreadyExists(name)){
            throw new IllegalArgumentException("This name is already in use");
        }

        //let's start by creating the object:
        DBTable<K, V> table = new DBTable<>(name, keyClass, valueClass);
        //set the object's types:
        Globals.addNameTable(name, table);
        Globals.addTableToThread(name);
        //send it to TxMgrImpl:
        //writeToDisk(table, name);
        return (Map<K, V>) Globals.getTable(name);
    }



    /**
     * Sets the duration of the "transaction timeout".  A client whose
     * transaction's duration exceeds the DBMS's timeout will be automatically
     * rolled back by the DBMS.
     *
     * @param ms the timeout duration in ms, must be greater than 0
     */
    @Override
    public void setTxTimeoutInMillis(int ms) {
        Globals.lockingTimeout = ms;
    }

    /**
     * Returns the current DBMS transaction timeout duration.
     *
     * @return duration in milliseconds
     */
    @Override
    public int getTxTimeoutInMillis() {
        return Globals.lockingTimeout;
    }

    @Override
    public double getDiskUsageInMB(){
        return Globals.getStorageSize() / 1000000.0;
    }

    @Override
    public void clear(){
        Globals.clearStorage();
        Globals.clearTablesInMemory();
    }
}


