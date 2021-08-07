package edu.yu.oatsdb.v1;

import com.vmlens.api.AllInterleavings;
import edu.yu.oatsdb.base.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;


import static org.junit.Assert.assertEquals;

public class VmlensMultithreadTest {
    static DBMS db;
    static TxMgr txMgr;
    static Map<Character, String> objectMap;
    static AtomicBoolean killProcess = new AtomicBoolean(false);
    private final static Logger logger = LogManager.getLogger(VmlensMultithreadTest.class);

    static class SetMapObj<K,V> implements Runnable {
        final String name;
        final Character key;
        final String value;
        /**
         * @param mapName Name of the "map" (SQL table)
         * @param key The key. Must be of type Object
         * @param value The value. Must be of type Object
         */
        public SetMapObj(String mapName, Character key, String value) {
            this.name = mapName;
            this.key = key;
            this.value = value;
        }

        @Override
        public void run() {
            try {
                txMgr.begin();
                objectMap = db.getMap(name, Character.class, String.class);
                objectMap.put(key, value);
                txMgr.commit();
            } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                    | SystemException | ClientTxRolledBackException e) {
                e.printStackTrace();
                Runtime.getRuntime().halt(-34);
                killProcess.set(true);
                e.printStackTrace();
                throw new RuntimeException();
            } catch (Exception e){ Thread.currentThread().stop();};

        }
    }
    static class GetMapObj<K> implements Runnable {
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
                objectMap = db.getMap(mapName, Character.class, String.class);
                objectMap.get(key);
                txMgr.commit();
            } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                    | SystemException | ClientTxRolledBackException e) {
                e.printStackTrace();
                Runtime.getRuntime().halt(-34);
                killProcess.set(true);
                e.printStackTrace();
                throw new RuntimeException();
            } catch (Exception e){ Thread.currentThread().stop();};

        }
    }

    @BeforeClass
    public static void before() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        db = OATSDBType.dbmsFactory(OATSDBType.V1);
        txMgr = OATSDBType.txMgrFactory(OATSDBType.V1);
        txMgr.begin() ;
        objectMap = db.createMap("obj", Character.class, String.class);
        txMgr.commit();

    }

    @Test
    public void simpleTwoThreadConcurrentPut() throws ExecutionException, InterruptedException, SystemException, NotSupportedException, RollbackException {
        try (AllInterleavings allInterleavings = AllInterleavings.builder("2ConcurrentPut").maximumSynchronizationActionsPerThread(2000).build()){
            int i = 0;
            while(allInterleavings.hasNext() && !killProcess.get()){
                System.out.println("==================================RUN "+(i++)+"==========================================");
                Thread first = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.put('1', "Set");
                        txMgr.commit();
                    } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                            | SystemException | ClientTxRolledBackException e) {
                        e.printStackTrace();
                        Runtime.getRuntime().halt(-34);
                        killProcess.set(true);
                        e.printStackTrace();
                        throw new RuntimeException();
                    } catch (Exception e){ Thread.currentThread().stop();};
                });
                Thread second = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.put('2', "Set");
                        txMgr.commit();
                    } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                            | SystemException | ClientTxRolledBackException e) {
                        e.printStackTrace();
                        Runtime.getRuntime().halt(-34);
                        killProcess.set(true);
                        e.printStackTrace();
                        throw new RuntimeException();
                    } catch (Exception e){ Thread.currentThread().stop();};
                });
                // Wait until all threads are finish
                first.setName("First Thread");
                second.setName("Second Thread");
                first.start();
                second.start();
                first.join();
                second.join();

                txMgr.begin();
                objectMap.remove('1');
                objectMap.remove('2');
                txMgr.commit();
            }
        }
    }

    //@Test
    public void simpleTwoThreadConcurrentGet() throws ExecutionException, InterruptedException, SystemException, NotSupportedException, RollbackException {
        try (AllInterleavings allInterleavings = new AllInterleavings("2ConcurrentGet")){
            while(allInterleavings.hasNext()){
                Thread first = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.get('1');
                        txMgr.commit();
                    } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                            | SystemException | ClientTxRolledBackException e) {
                        e.printStackTrace();
                        Runtime.getRuntime().halt(-34);
                        killProcess.set(true);
                        e.printStackTrace();
                        throw new RuntimeException();
                    } catch (Exception e){ Thread.currentThread().stop();};
                });
                Thread second = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.get('2');
                        txMgr.commit();
                    } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                            | SystemException | ClientTxRolledBackException e) {
                        e.printStackTrace();
                        Runtime.getRuntime().halt(-34);
                        killProcess.set(true);
                        e.printStackTrace();
                        throw new RuntimeException();
                    } catch (Exception e){ Thread.currentThread().stop();};
                });
                first.setName("First Thread");
                second.setName("Second Thread");
                // Wait until all threads are finish
                first.start();
                second.start();
                first.join();
                second.join();

            }
        }
    }

    //@Test
    public void simpleTwoThreadConcurrentGetMultipleTx() throws ExecutionException, InterruptedException, SystemException, NotSupportedException, RollbackException {
        int i = 0;
        try (AllInterleavings allInterleavings = new AllInterleavings("2ConcurrentGetMultipleTx")){
            while(allInterleavings.hasNext()){
                System.out.println("==================================RUN "+(i++)+"==========================================");

                Thread first = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.get('1');
                        txMgr.commit();
                        txMgr.begin();
                        objectMap.get('2');
                        txMgr.commit();
                    } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                            | SystemException | ClientTxRolledBackException e) {
                        e.printStackTrace();
                        Runtime.getRuntime().halt(-34);
                        killProcess.set(true);
                        e.printStackTrace();
                        throw new RuntimeException();
                    } catch (Exception e){ Thread.currentThread().stop();};
                });
                Thread second = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.get('1');
                        txMgr.commit();
                        txMgr.begin();
                        objectMap.get('2');
                        txMgr.commit();
                    } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                            | SystemException | ClientTxRolledBackException e) {
                        e.printStackTrace();
                        Runtime.getRuntime().halt(-34);
                        killProcess.set(true);
                        e.printStackTrace();
                        throw new RuntimeException();
                    } catch (Exception e){ Thread.currentThread().stop();};
                });
                // Wait until all threads are finish
                first.start();
                second.start();
                first.join();
                second.join();


            }
        }
    }
    //@Test
    public void simpleTwoThreadConcurrentPutMultipleTx() throws ExecutionException, InterruptedException, SystemException, NotSupportedException, RollbackException {
            int i =0;
        try (AllInterleavings allInterleavings = new AllInterleavings("2ConcurrentPutMultipleTx")){
            while(allInterleavings.hasNext()){
                System.out.println("==================================RUN "+(i++)+"==========================================");

                Thread first = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.put('1', "Set");
                        txMgr.commit();
                        txMgr.begin();
                        objectMap.put('2', "Set");
                        txMgr.commit();
                    } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                            | SystemException | ClientTxRolledBackException e) {
                        e.printStackTrace();
                        Runtime.getRuntime().halt(-34);
                        killProcess.set(true);
                        e.printStackTrace();
                        throw new RuntimeException();
                    } catch (Exception e){ Thread.currentThread().stop();};
                });
                Thread second = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.put('1', "Set");
                        txMgr.commit();
                        txMgr.begin();
                        objectMap.put('2', "Set");
                        txMgr.commit();
                    } catch (ClientNotInTxException | RollbackException | IllegalStateException | NotSupportedException
                            | SystemException | ClientTxRolledBackException e) {
                        e.printStackTrace();
                        Runtime.getRuntime().halt(-34);
                        killProcess.set(true);
                        e.printStackTrace();
                        throw new RuntimeException();
                    } catch (Exception e){ Thread.currentThread().stop();};
                });
                // Wait until all threads are finish
                first.start();
                second.start();
                first.join();
                second.join();

                txMgr.begin();
                Assert.assertEquals("Set", objectMap.remove('1'));
                Assert.assertEquals("Set", objectMap.remove('2'));
                txMgr.commit();

            }
        }
    }

}
