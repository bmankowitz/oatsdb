package edu.yu.oatsdb.v1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
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
    private static volatile HashMap<String, DBTable> nameTables = new HashMap<>(); //mapping of name to tables
    protected static volatile HashMap<Thread, TxImpl> threadTxMap = new HashMap<>(); //mapping of thread to transaction
    protected static volatile HashMap<Thread, ArrayList<DBTable>> threadTableMap = new HashMap<>(); //mapping of thread to DBTable(s)
    private final static Logger logger = LogManager.getLogger(Globals.class);
    public static int lockingTimeout = 5000;

    protected static synchronized void addToTx(String name, Thread thread){
        //logger.info("Adding table {} to tx {}", name, thread);
        DBTable temp = nameTables.get(name);
        if(temp.isDirty()) {
            return;
        }
        temp.initializeTable();
        ArrayList<DBTable> existingTablesInThread = threadTableMap.get(thread);
        //if this is the first table, create the arraylist
        if(existingTablesInThread == null) existingTablesInThread = new ArrayList<DBTable>();
        existingTablesInThread.add(temp);
        threadTableMap.put(thread, existingTablesInThread);
        nameTables.replace(name, temp);
    }

    protected static void commitThreadTables(Thread thread){
        //logger.info("Committing threads (GLOBALS) for {}, id= {}", thread, thread.getId());
        ArrayList<DBTable> tables = threadTableMap.remove(thread);
        //if this thread didn't touch any tables, nothing to commit
        if(tables == null) return;
        for(DBTable table : tables){
            table = nameTables.get(table.getName());
            table.commitCurrentTable();
            nameTables.replace(table.getName(), table);
        }
    }

    protected static void revertThreadTables(Thread thread){
        ArrayList<DBTable> tables = threadTableMap.get(thread);
        //if this thread didn't touch any tables, nothing to commit
        if(tables == null) return;
        for(DBTable table : tables){
            table = nameTables.get(table.getName());
            table.rollbackCurrentTable();
            nameTables.replace(table.getName(), table);
        }
    }
}
