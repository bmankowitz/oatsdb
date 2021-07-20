package edu.yu.oatsdb.v1;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("all")
public class Globals {
    protected static boolean alreadyExists(String name){
        return nameTables.keySet().contains(name);
    }

    public static void addNameTable(String name, DBTable newTable) {
        nameTables.put(name, newTable);
    }
    public static DBTable getTable(String name){
        return nameTables.get(name);
    }
    private static HashMap<String, DBTable> nameTables = new HashMap<>(); //mapping of name to tables tables
    protected static HashMap<Thread, TxImpl> threadTxMap = new HashMap<>();
    protected static HashMap<Thread, DBTable> threadTableMap = new HashMap<>();
    public static int lockingTimeout = 5000;

    protected static String getNameFromTable(DBTable needle){
        for(Map.Entry entry : nameTables.entrySet()){
            if(entry.getValue().equals(needle)) return (String) entry.getKey();
        }
        return null;
    }
    protected static void addToTx(String name){
        DBTable temp = nameTables.get(name);
        if(temp.isDirty()) {
            return;
        }
        temp.startTx();
        threadTableMap.put(Thread.currentThread(), temp);
        nameTables.replace(name, temp);
    }

    @Deprecated
    protected static void startAll(){
        DBTable temp;
        for(String name : nameTables.keySet() ){
            temp = nameTables.get(name);
            temp.startTx();
            nameTables.replace(name, temp);
        }

    }
    @Deprecated
    protected static void commitAll(){
        DBTable temp;
        for(String name : nameTables.keySet() ){
            temp = nameTables.get(name);
            temp.commitCurrentTable();
            nameTables.replace(name, temp);
        }
    }
    @Deprecated
    protected static void revertAll(){
        DBTable temp;
        for(String name : nameTables.keySet() ){
            temp = nameTables.get(name);
            temp.rollbackCurrentTable();
            nameTables.replace(name, temp);
        }
    }
}
