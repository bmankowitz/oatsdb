package edu.yu.oatsdb.v1;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@SuppressWarnings("all")
public class Globals {


    private static volatile ConcurrentHashMap<String, DBTable> nameTables = new ConcurrentHashMap<>(); //mapping of name to tables
    protected static volatile ConcurrentHashMap<Thread, TxImpl> threadTxMap = new ConcurrentHashMap<>(); //mapping of thread to transaction
    // FIXME: 7/27/2021 Replace ArrayList<DBTable> with thread safe concurrent queue
    protected static volatile ConcurrentHashMap<Thread, ConcurrentLinkedQueue<DBTable>> threadTableMap = new ConcurrentHashMap<>(); //mapping of thread to DBTable(s)
    public static final boolean log = true;
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

    protected static void addTableToThread(String name){
        if (Globals.log) logger.info("Adding table {} to tx {}", name, Thread.currentThread());
        if(nameTables.get(name).isDirty()) {
            return;
        }

//        nameTables.get(name).ensureTableIsClean();
//        threadTableMap.compute(Thread.currentThread(), (key, value) ->{
//            if(value == null) return new ConcurrentLinkedQueue<>(Arrays.asList(nameTables.get(name)));
//            //otherwise just add in the value
//            value.add(nameTables.get(name));
//            return value;
//        });


        if(threadTableMap.contains(Thread.currentThread())){
            threadTableMap.get(Thread.currentThread()).add(nameTables.get(name));
        }
        //This is a new thread


        ConcurrentLinkedQueue<DBTable> existingTablesInThread = threadTableMap.get(Thread.currentThread());
        //if this is the first table, create the arraylist
        if(existingTablesInThread == null) existingTablesInThread = new ConcurrentLinkedQueue<DBTable>();
        existingTablesInThread.add(nameTables.get(name));
        threadTableMap.put(Thread.currentThread(), existingTablesInThread);
        nameTables.get(name).ensureTableIsClean();

    }

    protected static void commitThreadTables(){
//        if (Globals.log) logger.info("Committing threads (GLOBALS) for {}, id= {}", Thread.currentThread() );
//        threadTableMap.computeIfPresent(Thread.currentThread(), (key, value) ->{
//            value.forEach(DBTable::commitCurrentTable);
//            return null;
//        });

        ConcurrentLinkedQueue<DBTable> tables = threadTableMap.remove(Thread.currentThread());
        //if this thread didn't touch any tables, nothing to commit
        if(tables == null) return;
        for(DBTable table : tables){
            table.commitCurrentTable();
        }
    }

    protected static void revertThreadTables(){
//        threadTableMap.computeIfPresent(Thread.currentThread(), (key, value) ->{
//            value.forEach(DBTable::rollbackCurrentTable);
//            return null;
//        });
        ConcurrentLinkedQueue<DBTable> tables = threadTableMap.get(Thread.currentThread());
        //if this thread didn't touch any tables, nothing to commit
        if(tables == null) return;
        for(DBTable table : tables){
            table.rollbackCurrentTable();
        }
    }
}
