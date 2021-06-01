package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.*;

import java.io.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;

public enum DBMSImpl implements DBMS {
    Instance;
    //private HashMap<String, DBTable> tables = new HashMap<String, DBTable>();

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(String name, Class<K> keyClass, Class<V> valueClass) {
        //ensure client is in a transaction
        try {
            if (Globals.threadTxMap.get(Thread.currentThread()) == null ||
                    Globals.threadTxMap.get(Thread.currentThread()).status != TxStatus.ACTIVE) {
                throw new ClientNotInTxException("Not in a Tx");
            }
        }catch (NullPointerException e){
            throw new ClientNotInTxException("currentTx is null");
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


        Globals.addToTx(name);
        return Globals.getTable(name);
    }

    public <K, V> Map<K, V> createMap(String name, Class<K> keyClass, Class<V> valueClass) {
        //basic set-up: since this needs to be persistent, save each new Map to a folder.
        //Within the folder, there is the original Map and a new file for each transaction.

        //Create the object and write to file.

        //ensure this is not for a rolledback tx:
        //ensure we are in a Tx:
        try {
            if (Globals.threadTxMap.get(Thread.currentThread()) == null ||
                    Globals.threadTxMap.get(Thread.currentThread()).status != TxStatus.ACTIVE) {
                throw new ClientNotInTxException("Not in a Tx");
            }
        }catch (NullPointerException e){
            throw new ClientNotInTxException("currentTx is null");
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
        DBTable<K, V> table = new DBTable<>();
        //set the object's types:
        table.keyClass = keyClass;
        table.valueClass = valueClass;
        Globals.addNameTable(name, table);
        Globals.addToTx(name);
        //send it to TxMgrImpl:
        //writeToDisk(table, name);
        return Globals.getTable(name);
    }

    private <K, V> void writeToDisk(DBTable<K, V> table, String name){
        //now create folder and serialize. See https://www.tutorialspoint.com/java/java_serialization.htm
        try {
            File file = new File(name);
            if(!file.mkdir()) {
                //throw new RuntimeException();
                System.out.println();
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

    public static void main(String[] args) {

    }
}


