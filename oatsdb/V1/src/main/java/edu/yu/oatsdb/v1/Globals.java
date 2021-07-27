package edu.yu.oatsdb.v1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("all")
public class Globals {


    private static volatile ConcurrentHashMap<String, DBTable> nameTables = new ConcurrentHashMap<>(); //mapping of name to tables
    protected static volatile ConcurrentHashMap<Thread, TxImpl> threadTxMap = new ConcurrentHashMap<>(); //mapping of thread to transaction
    // FIXME: 7/27/2021 Replace ArrayList<DBTable> with thread safe concurrent queue
    protected static volatile ConcurrentHashMap<Thread, ArrayList<DBTable>> threadTableMap = new ConcurrentHashMap<>(); //mapping of thread to DBTable(s)
    private final static Logger logger = LogManager.getLogger(Globals.class);
    public static int lockingTimeout = 5000;

    protected static boolean alreadyExists(String name){
        return nameTables.keySet().contains(name);
    }
    public static void addNameTable(String name, DBTable newTable) {
        nameTables.put(name, newTable);
    }
    public static DBTable getTable(String name){
        return nameTables.get(name);
    }

    protected static void addTableToThread(String name, Thread thread){
        //logger.info("Adding table {} to tx {}", name, thread);
        if(nameTables.get(name).isDirty()) {
            return;
        }
        nameTables.get(name).ensureTableIsClean();

        ArrayList<DBTable> existingTablesInThread = threadTableMap.get(thread);
        //if this is the first table, create the arraylist
        if(existingTablesInThread == null) existingTablesInThread = new ArrayList<DBTable>();
        existingTablesInThread.add(nameTables.get(name));
        threadTableMap.put(thread, existingTablesInThread);
        nameTables.get(name).ensureTableIsClean();

    }

    protected static void commitThreadTables(Thread thread){
        //System.out.println("Committing for thread: " + thread);
        //logger.info("Committing threads (GLOBALS) for {}, id= {}", thread, thread.getId());
        ArrayList<DBTable> tables = threadTableMap.remove(thread);
        //if this thread didn't touch any tables, nothing to commit
        if(tables == null) return;
        for(DBTable table : tables){
            //System.out.println(thread+ ": Committing table: " + table.tableName);
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
