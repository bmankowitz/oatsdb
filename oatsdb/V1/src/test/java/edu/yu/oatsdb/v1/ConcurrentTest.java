package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.*;

import java.util.concurrent.*;
import java.util.Map;
import java.util.ArrayList;
import static org.junit.Assert.*;



public class ConcurrentTest {
    ConfigurableDBMS db;
    TxMgr txMgr;
    int i;
    Map<Character, String> gradeDetail;
    Map<Object, Object> objectMap;
    Map<Object, Object> objectMap2;
    private final static Logger logger = LogManager.getLogger(ConcurrentTest.class);

    /**This initializer method creates the OATS database and Transaction Manager,
     * as well as creating three different empty maps.
     * @throws InstantiationException
     * @throws SystemException
     * @throws NotSupportedException
     * @throws RollbackException
     */
    @Before
    public void before() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        db = (ConfigurableDBMS) OATSDBType.dbmsFactory(OATSDBType.V1);
        txMgr = OATSDBType.txMgrFactory(OATSDBType.V1);
        txMgr.begin() ;
        objectMap = db.createMap("obj", Object.class, Object.class);
        objectMap2 = db.createMap("obj2", Object.class, Object.class);
        txMgr.commit();

    }
    @After
    public void after(){
        //TODO: Create a clear method that will destroy the database and transactions
    }


    class SetMapObj<K,V> implements Runnable {
        final String name;
        final K key;
        final V value;
        /**
         * @param mapName Name of the "map" (SQL table)
         * @param key The key. Must be of type Object
         * @param value The value. Must be of type Object
         */
        public SetMapObj(String mapName, K key, V value) {
            this.name = mapName;
            this.key = key;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                txMgr.begin();
                objectMap = db.getMap(name, Object.class, Object.class);
                objectMap.put(key, value);
                txMgr.commit();
            } catch (NotSupportedException | SystemException | RollbackException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }

        }
    }
    class GetMapObj<K> implements Runnable {
        final String mapName;
        final Object key;

        public GetMapObj(String mapName, Object key) {
            this.mapName = mapName;
            this.key = key;
        }

        @Override
        public void run() {
            try {
                txMgr.begin();
                objectMap = db.getMap(mapName, Object.class, Object.class);
                objectMap.get(key);
                txMgr.commit();
            } catch (NotSupportedException | SystemException | RollbackException e) {
                e.printStackTrace();
                throw new RuntimeException();
            }

        }
    }

    @Test
    public void basicNoConflictPut() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 2;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable addA = new SetMapObj("obj", 'A', "Set");
        Runnable addB = new SetMapObj("obj", 'B', "Set");
        Future<Void> future = (Future<Void>) executor.submit(addA);
        Future<Void> future1 = (Future<Void>) executor.submit(addB);
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");

        future.get();
        future1.get();

        txMgr.begin();
        assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('A'));
        assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('B'));
        txMgr.commit();
    }
    @Test
    public void basicNoConflictPutRepeating() throws SystemException, NotSupportedException, RollbackException, InterruptedException, ExecutionException {
        for(int i = 0; i < 20000; i++) basicNoConflictPut();
    }
    @Test
    public void NSimultaneousNoConflictPutSameMap() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        // FIXME: 7/20/2021 STILL OCCASIONALLY FAILS
        int MY_THREADS = 2500;
        int NUM_TIMES = 2500;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        ArrayList<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < NUM_TIMES; i++) {
            Runnable setI = new SetMapObj("obj", i, "Set");
            futures.add((Future<Void>) executor.submit(setI));
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");
        for(Future<Void> future : futures){
            future.get();
        }
        for (int i = 0; i < NUM_TIMES; i++) {
            txMgr.begin();
            assertEquals("Set", db.getMap("obj", Object.class, Object.class).get(i));
            txMgr.commit();
        }
    }
    @Test
    public void NSimultaneousNoConflictPutSeparateMaps() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        // FIXME: 7/19/2021 FAILS SOMETIMES. UNCLEAR WHY
        int MY_THREADS = 10000;
        int NUM_TIMES = 10000;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        ArrayList<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < NUM_TIMES; i++) {
            Runnable setI = new SetMapObj("obj", i, "Set");
            Runnable setI2 = new SetMapObj("obj2", i, "Set");
            futures.add((Future<Void>) executor.submit(setI));
            futures.add((Future<Void>) executor.submit(setI2));
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");
        for(Future<Void> future : futures){
            future.get();
        }
        for (int i = 0; i < NUM_TIMES; i++) {
            txMgr.begin();
            assertEquals("Set", db.getMap("obj", Object.class, Object.class).get(i));
            assertEquals("Set", db.getMap("obj2", Object.class, Object.class).get(i));
            txMgr.commit();
        }
    }

    @Test
    public void SimultaneousFasterThanSequentialPut() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 2000;
        int NUM_TIMES = 2000;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        ArrayList<Future<Void>> futures = new ArrayList<>();
        double simulStart = System.nanoTime();
        for (int i = 0; i < NUM_TIMES; i++) {
            Runnable setI = new SetMapObj("obj", i, "Set");
            futures.add((Future<Void>) executor.submit(setI));
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        double simulEnd = System.nanoTime();
        //now for single thread:
        double seqStart = System.nanoTime();
        for (int i = NUM_TIMES; i < NUM_TIMES*2; i++) {
            Runnable setI = new SetMapObj("obj", i, "Set");
            futures.add((Future<Void>) singleThread.submit(setI));
        }
        double seqEnd = System.nanoTime();
        for(Future<Void> future : futures){
            future.get();
        }
        for (int i = 0; i < NUM_TIMES*2; i++) {
            txMgr.begin();
            assertEquals("Set", db.getMap("obj", Object.class, Object.class).get(i));
            txMgr.commit();
        }
        double simulTime = (simulEnd-simulStart);
        double seqTime = (seqEnd-seqStart);
        System.out.println("Finished simultaneous threads in: " + simulTime);
        System.out.println("Sequential put took: " + seqTime);
        assertTrue(simulTime * 50 < seqEnd);
    }
    @Test
    public void SimultaneousFasterThanSequentialGet() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 2000;
        int NUM_TIMES = 2000;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        ExecutorService singleThread = Executors.newSingleThreadExecutor();
        ArrayList<Future<Void>> futures = new ArrayList<>();
        double simulStart = System.nanoTime();
        for (int i = 0; i < NUM_TIMES; i++) {
            Runnable getI = new GetMapObj("obj", i);
            futures.add((Future<Void>) executor.submit(getI));
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
        double simulEnd = System.nanoTime();
        //now for single thread:
        double seqStart = System.nanoTime();
        for (int i = NUM_TIMES; i < NUM_TIMES*2; i++) {
            Runnable getI = new GetMapObj("obj", i);
            futures.add((Future<Void>) singleThread.submit(getI));
        }
        double seqEnd = System.nanoTime();
        for(Future<Void> future : futures){
            future.get();
        }
        double simulTime = (simulEnd-simulStart);
        double seqTime = (seqEnd-seqStart);
        System.out.println("Finished simultaneous threads in: " + simulTime);
        System.out.println("Sequential put took: " + seqTime);
        assertTrue(simulTime * 50 < seqEnd);
    }
    @Test
    public void NSimultaneousThreadsMultipleTxPerThread() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 1000;
        int NUM_TIMES = 1000;
        //ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        ExecutorService executor = Executors.newCachedThreadPool();

        ArrayList<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < NUM_TIMES; i++) {
            Runnable setI = new Runnable() {
                @Override
                public void run() {
                    try {
                        txMgr.begin();
                        objectMap = db.getMap("obj", Object.class, Object.class);
                        objectMap.put('A', "First tx");
                        txMgr.commit();
                        txMgr.begin();
                        objectMap.put('B', "Second tx");
                        txMgr.commit();
                        txMgr.begin();
                        objectMap.put('C', "Third tx");
                        txMgr.commit();
                    } catch (NotSupportedException | SystemException | RollbackException  e) {
                        e.printStackTrace();
                        throw new RuntimeException();
                    }
                }
            };
            futures.add((Future<Void>) executor.submit(setI));
            //Thread.sleep(1);
        }
        executor.shutdown();
        // Wait until all threads are finish
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");
        for(Future<Void> future : futures){
            future.get();
        }
        for (int i = 0; i < NUM_TIMES; i++) {
            txMgr.begin();
            assertEquals("First tx", db.getMap("obj", Object.class, Object.class).get('A'));
            assertEquals("Second tx", db.getMap("obj", Object.class, Object.class).get('B'));
            assertEquals("Third tx", db.getMap("obj", Object.class, Object.class).get('C'));
            txMgr.commit();
        }
    }

    @Test
    public void txProgressCommitVisibleToOtherThread() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 2;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable addA = new Runnable() {
            @Override
            public void run() {
                try {
                    txMgr.begin();
                    System.out.println("Setting A");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('A', "Set");
                    System.out.println("Set A");
                    System.out.println("Sleeping");
                    Thread.sleep(50);
                    System.out.println("Awake");
                    txMgr.commit();
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Runnable addB = new Runnable() {
            @Override
            public void run() {
                try {
                    txMgr.begin();
                    System.out.println("Setting B");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('B', "Set");
                    System.out.println("Set B");
                    txMgr.commit();
                    txMgr.begin();
                    System.out.println("Checking A: ");
                    System.out.println(db.getMap("obj", Object.class, Object.class).get('A'));
                    assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('A'));
                    System.out.println("Checked A");
                    txMgr.commit();
                    Thread.sleep(50);
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Future<Void> future = (Future<Void>) executor.submit(addA);
        Future<Void> future1 = (Future<Void>) executor.submit(addB);
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");

        future.get();
        future1.get();

        txMgr.begin();
        assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('A'));
        assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('B'));
        //set back to null:
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.remove('A');
        objectMap.remove('B');
        txMgr.commit();

    }

    @Test
    public void txProgressInvisibleToOtherThreadMultipleMaps() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 2;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable addA = new Runnable() {
            @Override
            public void run() {
                try {
                    txMgr.begin();
                    logger.info("Setting A");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap2 = db.getMap("obj2", Object.class, Object.class);
                    objectMap.put('A', "Set");
                    objectMap2.put('A',"Set");
                    logger.info("Set A");
                    logger.info("About to sleep");
                    Thread.sleep(300);
                    logger.info("Awake");
                    logger.info("Committing A");
                    txMgr.commit();
                    logger.info("Committed A");
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Runnable addB = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(20);
                    txMgr.begin();
                    logger.info("Checking A");
                    assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('A'));
                    assertEquals("Set", db.getMap("obj2", Object.class, Object.class).get('A'));
                    logger.info("Committing B");
                    txMgr.commit();
                    logger.info("Committed B");
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Future<Void> future = (Future<Void>) executor.submit(addA);
        Future<Void> future1 = (Future<Void>) executor.submit(addB);
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");

        future.get();
        future1.get();

        txMgr.begin();
        assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('A'));
        assertEquals("Set", db.getMap("obj2", Object.class, Object.class).get('A'));
        //set back to null:
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.remove('A');
        objectMap2.remove('A');
        txMgr.commit();

    }
    @Test
    public void txProgressInvisibleToOtherThreadsRepeating() throws InterruptedException, RollbackException, NotSupportedException, ExecutionException, SystemException {
        for (int i = 0; i < 150; i++) {
            //txProgressInvisibleToOtherThread();
        }
        for (int i = 0; i < 150; i++) {
            System.out.println("Run: "+ i);
            txProgressInvisibleToOtherThreadMultipleMaps();
        }
    }

    @Test
    public void TwoTxAccessingSameElementUnderTimeout() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 2;
        txMgr.begin();
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.put('C', "Set");
        txMgr.commit();

        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable addA = new Runnable() {
            @Override
            public void run() {
                try {
                    txMgr.begin();
                    System.out.println("Start Set1");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('C', "Set1");
                    Thread.sleep(500);
                    txMgr.commit();
                    System.out.println("Finished Set1");

                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Runnable addB = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(250);
                    txMgr.begin();
                    System.out.println("Start Set2");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('C', "Set2");
                    txMgr.commit();
                    System.out.println("Finished Set2");
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    //throw new RuntimeException();
                }
            }
        };
        Future<Void> future = (Future<Void>) executor.submit(addA);
        Future<Void> future1 = (Future<Void>) executor.submit(addB);
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");

        future.get();
        future1.get();

        txMgr.begin();
        assertEquals("Set2", db.getMap("obj", Object.class, Object.class).get('C'));
        //set back to null:
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.remove('C');
        txMgr.commit();
    }
    @Test
    public void TwoTxAccessingSameElementUnderTimeoutRepeating() throws InterruptedException, RollbackException, NotSupportedException, ExecutionException, SystemException {
        for (int i = 0; i < 25; i++) {
            TwoTxAccessingSameElementUnderTimeout();
        }
    }
    @Test(expected = ClientTxRolledBackException.class)
    public void TwoTxAccessingSameElementTimeout() throws Throwable {
        int MY_THREADS = 2;
        txMgr.begin();
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.put('C', "Set");
        txMgr.commit();

        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable addA = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("set1");
                    txMgr.begin();
                    System.out.println("Started Set1");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('C', "Set1");
                    boolean test = true;
                    Thread.sleep(6000);
                    txMgr.commit();
                    System.out.println("Finished Set1");

                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Runnable addB = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("set2");
                    Thread.sleep(250);
                    txMgr.begin();
                    System.out.println("Started Set2");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('C', "Set2");
                    txMgr.commit();
                    System.out.println("Finished Set2");
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Future<Void> future = (Future<Void>) executor.submit(addA);
        Future<Void> future1 = (Future<Void>) executor.submit(addB);
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");
        try {
            future.get();
            future1.get();
        } catch (ExecutionException e){
            throw e.getCause();
        }
    }
    @Test
    public void TenTxAccessingSameElementTimeout() throws Throwable {
        int MY_THREADS = 15;
        int exceptions = 0;
        txMgr.begin();
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.put('C', "Set");
        txMgr.commit();

        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable addA = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("set1");
                    txMgr.begin();
                    System.out.println("Started Set1");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('C', "Set1");
                    Thread.sleep(6500);
                    txMgr.commit();
                    System.out.println("Finished Set1");

                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Runnable addB = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(10);
                    txMgr.begin();
                    System.out.println("Started otherSet");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('C', "Set2");
                    txMgr.commit();
                    System.out.println("Finished otherSEt");
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Future<Void> future = (Future<Void>) executor.submit(addA);
        ArrayList<Future<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            futures.add((Future<Void>) executor.submit(addB));
        }
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");
        try {
            future.get();
        } catch (Exception e){
            exceptions++;
        }
        for(Future<Void> lFuture : futures) {
            try {
                lFuture.get();
            } catch (Exception e){
                exceptions++;
            }
        }
        assertEquals(10, exceptions);
    }
    @Test
    public void ensureLockedKeyIsBlocking() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 2;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable addA = new Runnable() {
            @Override
            public void run() {
                try {
                    txMgr.begin();
                    objectMap.put("q", "Set");
                    long startTime = System.currentTimeMillis();
                    logger.info("Set a at {}", startTime);
                    while (System.currentTimeMillis() - startTime < 500){}
                    txMgr.commit();
                    logger.info("committed a");

                } catch (NotSupportedException | SystemException | RollbackException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Runnable addB = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(20);
                    txMgr.begin();
                    logger.info("Checking A");
                    double start = System.currentTimeMillis();
                    logger.info("Should now be waiting...");
                    assertEquals("Set", objectMap.get("q"));
                    double end = System.currentTimeMillis();
                    logger.info("Checked A. Now checking time:");
                    assertTrue( end - start < 1750);
                    txMgr.commit();
                    logger.info("Done!");
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Future<Void> future = (Future<Void>) executor.submit(addA);
        Future<Void> future1 = (Future<Void>) executor.submit(addB);
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");

        future.get();
        future1.get();

    }
    @Test//(expected = ClientTxRolledBackException.class)
    public void TxRollbackTimeoutCorrectlyRollsBack() throws Throwable {
        int MY_THREADS = 11;
        txMgr.begin();
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.put('C', "SetBefore");
        txMgr.commit();

        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable longCommit = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("set1");
                    txMgr.begin();
                    System.out.println("Started Set1");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('C', "SetLongCommit");
                    Thread.sleep(7500);
                    txMgr.commit();
                    System.out.println("Finished Set1");

                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Runnable shortCommit = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("set2");
                    Thread.sleep(250);
                    txMgr.begin();
                    System.out.println("Started Set2");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('C', "SetSecondCommit");
                    fail("This tx should have timed out by now");
                    txMgr.commit();
                    System.out.println("Finished Set2");
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    fail();
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Future<Void> longFuture = (Future<Void>) executor.submit(longCommit);
        Future<Void> shortFuture = (Future<Void>) executor.submit(shortCommit);
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");
        try {
            longFuture.get();
            shortFuture.get();
        } catch (ExecutionException e){
            if(e.getCause() instanceof ClientTxRolledBackException){
                //we passed the test. Do nothing
            }
            else throw e.getCause();
        }
        txMgr.begin();
        assertEquals("SetLongCommit", db.getMap("obj", Object.class, Object.class).get('C'));
        txMgr.commit();
    }
    @Test
    public void TxAllowsSingleThreadWorkBeyondTimeLimit() throws Throwable {
        int MY_THREADS = 11;
        txMgr.begin();
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.put('Y', "SetBefore");
        txMgr.commit();

        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        Runnable addA = new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.currentThread().setName("set1");
                    txMgr.begin();
                    System.out.println("Started Set1");
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.put('Y', "SetDuringLongCommit");
                    Thread.sleep(6000);
                    txMgr.commit();
                    System.out.println("Finished Set1");

                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };

        Future<Void> future = (Future<Void>) executor.submit(addA);
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");
        try {
            future.get();
        } catch (ExecutionException e){
            throw e.getCause();
        }
        txMgr.begin();
        assertEquals("SetDuringLongCommit", db.getMap("obj", Object.class, Object.class).get('Y'));

    }
    @Test
    public void txInProgressUpdatesWhenOtherThreadFinishes() throws SystemException, NotSupportedException, RollbackException, ExecutionException, InterruptedException {
        int MY_THREADS = 2;
        ExecutorService executor = Executors.newFixedThreadPool(MY_THREADS);
        txMgr.begin();
        objectMap = db.getMap("obj", Object.class, Object.class);
        objectMap.put('F', "NotSet");
        txMgr.commit();
        Runnable addA = new Runnable() {
            @Override
            public void run() {
                try {
                    txMgr.begin();
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    //System.out.println(db.getMap("obj", Object.class, Object.class).get('F'));
                    Thread.sleep(50);
                    assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('F'));
                    txMgr.commit();
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Runnable addB = new Runnable() {
            @Override
            public void run() {
                try {
                    txMgr.begin();
                    objectMap = db.getMap("obj", Object.class, Object.class);
                    objectMap.replace('F', "Set");
                    txMgr.commit();
                    Thread.sleep(5);
                } catch (NotSupportedException | SystemException | RollbackException | InterruptedException e) {
                    e.printStackTrace();
                    throw new RuntimeException();
                }
            }
        };
        Future<Void> future = (Future<Void>) executor.submit(addA);
        Future<Void> future1 = (Future<Void>) executor.submit(addB);
        // Wait until all threads are finish
        executor.shutdown();
        while (!executor.isTerminated()) {}
        System.out.println("\nFinished all threads");

        future.get();
        future1.get();

        txMgr.begin();
        assertEquals("Set", db.getMap("obj", Object.class, Object.class).get('F'));
        //set back to null:
        //objectMap.remove('F');
        txMgr.commit();

    }
    @Test
    public void txInProgressUpdatesWhenOtherThreadFinishesRepeating() throws InterruptedException, RollbackException, NotSupportedException, ExecutionException, SystemException {
        for (int i = 0; i < 250; System.out.println((i++))) {
            txInProgressUpdatesWhenOtherThreadFinishes();
        }
    }
}
