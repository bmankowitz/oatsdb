package edu.yu.oatsdb.v2;

import edu.yu.oatsdb.base.SystemException;
import edu.yu.oatsdb.v2.DBTableMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("all")
public class Globals {


    private static volatile ConcurrentHashMap<String, DBTable> nameTables = new ConcurrentHashMap<>(); //mapping of name to tables
    protected static volatile ConcurrentHashMap<Thread, TxImpl> threadTxMap = new ConcurrentHashMap<>(); //mapping of thread to transaction
    // FIXME: 7/27/2021 Replace ArrayList<DBTable> with thread safe concurrent queue
    protected static volatile ConcurrentHashMap<Thread, Set<DBTable>> threadTableMap = new ConcurrentHashMap<>(); //mapping of thread to DBTable(s)
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
        if (Globals.log) logger.info("Received request to add  table {} to tx {}", name, Thread.currentThread());
        if(nameTables.get(name).isDirty()) {
            if (Globals.log) logger.info("AlreadyExists- Skipping request to add  table {} to tx {}", name, Thread.currentThread());
            return;
        }


        if(threadTableMap.contains(Thread.currentThread())){
            threadTableMap.get(Thread.currentThread()).add(nameTables.get(name));
        }
        //This is a new thread


        Set<DBTable> existingTablesInThread = threadTableMap.get(Thread.currentThread());
        //if this is the first table, create the arraylist
        if(existingTablesInThread == null) existingTablesInThread = ConcurrentHashMap.newKeySet();
        existingTablesInThread.add(nameTables.get(name));
        threadTableMap.put(Thread.currentThread(), existingTablesInThread);
        nameTables.get(name).ensureTableIsClean();

    }

    protected static void commitThreadTables() throws SystemException{
//        if (Globals.log) logger.info("Committing threads (GLOBALS) for {}, id= {}", Thread.currentThread() );
//        threadTableMap.computeIfPresent(Thread.currentThread(), (key, value) ->{
//            value.forEach(DBTable::commitCurrentTable);
//            return null;
//        });
        HashMap<String, Long> masterMapping = new HashMap<>();
        Set<DBTable> tables = threadTableMap.remove(Thread.currentThread());
        //if this thread didn't touch any tables, nothing to commit
        if(tables == null) return;
        for(DBTable table : tables){
            table.commitCurrentTable();
            masterMapping.put(table.tableName, Thread.currentThread().getId());
        }
        //We assume this is an "atomic" write
        setMasterValues(masterMapping);
    }

    protected static void revertThreadTables(){
//        threadTableMap.computeIfPresent(Thread.currentThread(), (key, value) ->{
//            value.forEach(DBTable::rollbackCurrentTable);
//            return null;
//        });
        Set<DBTable> tables = threadTableMap.get(Thread.currentThread());
        //if this thread didn't touch any tables, nothing to commit
        if(tables == null) return;
        for(DBTable table : tables){
            table.rollbackCurrentTable();
        }
    }
    public static void clearTablesInMemory(){
        nameTables.forEach( (key, value) -> {
            value = null;
        });
    }
    public static void readStorage() throws SystemException{
        HashMap<String, Long> existingMasterMap;
        try {
            File dir = new File("storage");
            File file = new File (dir, "master");
            if(!dir.exists()){
                if(Globals.log) logger.debug("No storage directory. Aborting recovery.");
                return;
            }

            if(file.exists()) {
                if(Globals.log) logger.info("Master file exists");
                FileInputStream fis = new FileInputStream(file);
                ObjectInputStream ois = new ObjectInputStream(fis);
                existingMasterMap = (HashMap<String, Long>) ois.readObject();
                if(Globals.log) logger.info("Existing master file {}", existingMasterMap);
            }
            else{
                if(Globals.log) logger.info("Master file does not exist");
                return;
            }

            for(Map.Entry<String, Long> entry : existingMasterMap.entrySet()) {
                String key = entry.getKey();
                Long value = entry.getValue();
                String fileName = value + "-" + key;
                if(Globals.log) logger.info("Attempting to read file {}", fileName);
                //begin by reading in the table if it exists:
                File mapFile = new File(dir, fileName);
                FileInputStream fis = new FileInputStream(mapFile);
                ObjectInputStream ois = new ObjectInputStream(fis);
                DBTableMetadata metaTable = (DBTableMetadata) ois.readObject();
                //let's start by creating the object:
                DBTable table = new DBTable<>(metaTable.tableName, metaTable.keyClass, metaTable.valueClass);
                //set the object's types:
                Globals.addNameTable(metaTable.tableName, table);
                Globals.addTableToThread(metaTable.tableName);
                //now we need to add the existing values:
                table.putAll(metaTable.map);
                //DONE!
                if(Globals.log) logger.debug("Finished importing table {} with tid {} and MetaTable: {}",
                        metaTable.tableName, value, metaTable);
            }

        } catch (IOException | ClassNotFoundException e){ throw new SystemException(e.toString()); }
    }
    public static long getStorageSize() {
        long length = 0;
        File dir = new File("storage");
        File[] files = dir.listFiles();

        if(files != null){
            for(File file : files){
                length += file.length();
            }
        }
        return length;
    }
    public static void clearStorage(){
        File dir = new File("storage");
        File[] files = dir.listFiles();
        if(files != null){
            for(File file : files){
                if(Globals.log) logger.debug("Deleting file {}", file.getName());
                file.delete();
            }
            dir.delete();
        } else {
            if(Globals.log) logger.debug("No files to delete");
        }
        if(Globals.log) logger.debug("Done deleting files");
    }
    public synchronized static void writeTempMap(DBTableMetadata table) throws SystemException{
        StringBuilder fileName = new StringBuilder();
        fileName.append(Thread.currentThread().getId()).append("-").append(table.tableName);
        try{
            File dir = new File("storage");
            File file = new File (dir, fileName.toString());
            if(!dir.exists()){
                dir.mkdir();
                if(Globals.log) logger.debug("creating storage dir");
            }
            FileOutputStream fos = new FileOutputStream(file);
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(table);
            oos.close();
            if(Globals.log) logger.debug("Finished writing file {} with values {}", fileName.toString(), table);
        } catch (IOException e){
            throw new SystemException(e.toString());
        }
    }
    public static void setMasterValues(HashMap<String, Long> mapping) throws SystemException{
        HashMap<String, Long> existingMap;
        try {
            final File dir = new File("storage");
            final File file = new File (dir, "master");
            if(!dir.exists()){
                dir.mkdir();
                if(Globals.log) logger.debug("creating storage dir");
            }

            if(file.exists()) {
                if(Globals.log) logger.info("Master file exists");
                FileInputStream fis = new FileInputStream(file);
                ObjectInputStream ois = new ObjectInputStream(fis);
                existingMap = (HashMap<String, Long>) ois.readObject();
                if(Globals.log) logger.info("Existing master file {}", existingMap);
                ois.close();
                existingMap.putAll(mapping);
            }
            else{
                if(Globals.log) logger.info("Master file does not exist");
                existingMap = new HashMap<>(mapping);
            }

            final FileOutputStream fos = new FileOutputStream(file);
            final ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(existingMap);
            oos.close();
            if(Globals.log) logger.info("Finished writing master: {}", existingMap);

        } catch (IOException | ClassNotFoundException e){ throw new SystemException(e.toString()); }

    }
}
