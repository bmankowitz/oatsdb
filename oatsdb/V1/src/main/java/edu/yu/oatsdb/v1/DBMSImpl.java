package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.*;

import java.io.*;
import java.util.Map;
import java.util.NoSuchElementException;

public enum DBMSImpl implements DBMS, ConfigurableDBMS {
    Instance;

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
            throw new NoSuchElementException("Unable to locate map with that name");
        }

        //ensure classes match
        DBTable<K, V> table = Globals.getTable(name);
        if(keyClass != table.keyClass || valueClass != table.valueClass){
            throw new ClassCastException("Tried to get map with different classes than on file");
        }


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

    private <K, V> void writeToDisk(DBTable<K, V> table, String name){
        //now create folder and serialize. See https://www.tutorialspoint.com/java/java_serialization.htm
        try {
            File file = new File(name);
            if(!file.mkdir()) {
                //throw new RuntimeException();
            }
            FileOutputStream fileOut = new FileOutputStream(name+"/"+name+".ser", false);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(table);
            out.close();
            fileOut.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
}


