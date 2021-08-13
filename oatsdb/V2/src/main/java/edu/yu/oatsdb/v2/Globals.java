package edu.yu.oatsdb.v2;

import edu.yu.oatsdb.base.SystemException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@SuppressWarnings("all")
public class Globals {


    private static volatile ConcurrentHashMap<String, DBTable> nameTables = new ConcurrentHashMap<>(); //mapping of name to tables
    protected static volatile ConcurrentHashMap<Thread, TxImpl> threadTxMap = new ConcurrentHashMap<>(); //mapping of thread to transaction
    // FIXME: 7/27/2021 Replace ArrayList<DBTable> with thread safe concurrent queue
    protected static volatile ConcurrentHashMap<Thread, Set<DBTable>> threadTableMap = new ConcurrentHashMap<>(); //mapping of thread to DBTable(s)
    public static final boolean log = true;
    private final static Logger logger = LogManager.getLogger(Globals.class);
    public static int lockingTimeout = 5000;
    public static final AtomicInteger txIdGenerator = new AtomicInteger(0);
    public static final AtomicInteger commitOrdinal = new AtomicInteger(0);

    private final static File storageDir = new File("storage");
    private final static Set<Integer> excludedCommits = ConcurrentHashMap.newKeySet();

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

        Set<DBTable> tables = threadTableMap.remove(Thread.currentThread());
        //if this thread didn't touch any tables, nothing to commit
        if(tables == null) return;
        for(DBTable table : tables){
            table.commitCurrentTable();
        }

        //We assume this is an "atomic" write
        setCommitted(Globals.threadTxMap.get(Thread.currentThread()).id);
    }

    protected static void revertThreadTables(){

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
        nameTables.clear();
        txIdGenerator.set(0);
        threadTxMap.clear();
        threadTableMap.clear();
    }
    public static void readStorage(){
        //general plan: get a list of every map.
        //Filter out the maps that don't have a matching commit file
        //sort by the TxID (or perhaps add in a new commitID (on commit not creation))
        //create a new DB table for each map, then add all of them.
        try {
            File dir = new File("storage");
            if(!dir.exists()){
                if(Globals.log) logger.info("No storage directory. Aborting recovery.");
                return;
            }
            HashMap<String, ArrayList<DBTableMetadata>> mapCommits = new HashMap<String, ArrayList<DBTableMetadata>>();
            List<String> allFileNames = Arrays.asList(dir.list());
            HashSet<String> mapNames = new HashSet<>();
            HashSet<String> validCommitIDs = new HashSet<>();
            ArrayList<String> verifiedCommitNames = new ArrayList<>();
            allFileNames.forEach( (fileName) -> {
                if(fileName.matches("[0-9]+-[\\w]+")){
                    String name = fileName.substring(fileName.indexOf('-')+1, fileName.length());
                    mapNames.add(name);
                }
                if(fileName.matches("[0-9]+$")) validCommitIDs.add(fileName);
            });
            //add the mapNames to the commitmap hashmap
            for (String name : mapNames) {
                mapCommits.put(name, new ArrayList<>());
            }

            //now that we have all valid commits, need to remove invalid ones
            for (String allFileName : allFileNames) {
                StringTokenizer strtok = new StringTokenizer(allFileName, "-");
                if (validCommitIDs.contains(strtok.nextToken())) {
                    if (strtok.hasMoreTokens()) {
                        if (Globals.log) logger.debug("Trying to add file {}", allFileName);
                            File file = new File(dir, allFileName);
                            FileInputStream fis = new FileInputStream(file);
                            ObjectInputStream ois = new ObjectInputStream(fis);
                            DBTableMetadata thisTable = (DBTableMetadata) ois.readObject();
                            mapCommits.get(strtok.nextToken()).add(thisTable);
                            continue;
                    }
                    if (Globals.log) logger.info("Skipping unverified file {}", allFileName);
                }
                if (Globals.log) logger.debug("Skipping commit verfication file {}", allFileName);
            }
            //now we have a list of map names and all of their commits. Time to create it:
            for(Map.Entry<String, ArrayList<DBTableMetadata>> entry : mapCommits.entrySet()) {
                String mapName = entry.getKey();
                if(Globals.log) logger.info("Starting to import commits on map {}", mapName);
                //Sort by commitOrdinal:
                Collections.sort(entry.getValue());
                //create a table
                if(entry.getValue().isEmpty()) continue;
                DBTableMetadata first = entry.getValue().get(0);

                DBTable table = new DBTable<>(first.tableName, first.keyClass, first.valueClass);
                //set the object's types:
                Globals.addNameTable(first.tableName, table);
                Globals.addTableToThread(first.tableName);
                //begin by reading in every commit:
                for(DBTableMetadata metaTable: entry.getValue()){
                    if(Globals.log) logger.info("Inserting txID {} for map {}", metaTable.txID, mapName);
                    table.putAll(metaTable.map);
                };

                //DONE!
                if(Globals.log) logger.info("Finished importing table {}",
                        mapName);
            }
        } catch (IOException | ClassNotFoundException e){
            logger.error("IO error while trying to load data from disk. Aborting recovery and deleting files.");
            logger.error(e);
            clearStorage(); }
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
    public static void writeTempMap(DBTableMetadata table) throws SystemException{
        StringBuilder fileName = new StringBuilder();
        fileName.append(table.txID).append("-").append(table.tableName);
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
    private static void createStorageIfNotExists() throws SystemException {
        if (!storageDir.exists()) {
            if (Globals.log) logger.debug("creating storage dir");
            storageDir.mkdir();
        }
    }


    private static void setCommitted(final int txID) throws SystemException{
        //the txIDs are INVALID tx, not valid ones.
        final File testMultipartCommit = new File(storageDir, Integer.toString(txID));
        createStorageIfNotExists();

        if (Globals.log) logger.info("Adding new txID {}", txID);


        //basic plan: first write out the tables 144-grades, 144-obj, etc.
        //Then, when everything is written out, add in the tx number. This will ensure the Tx is valid.
        // example: 144
        //may be a good idea to collate everything at the end.
        try {
            testMultipartCommit.createNewFile();
            if (Globals.log) logger.info("Finished writing commit: {}", testMultipartCommit.getName());
        } catch (IOException e){
            if(Globals.log) logger.error("Encountered error writing file {}:  {}",
                    testMultipartCommit.getName(), e.getMessage());
            throw new SystemException("Error reading file: \n" + e.toString());
        }

    }

}

