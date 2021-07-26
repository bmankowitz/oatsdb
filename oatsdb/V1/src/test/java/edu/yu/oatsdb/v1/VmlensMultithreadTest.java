package edu.yu.oatsdb.v1;

import com.vmlens.api.AllInterleavings;
import edu.yu.oatsdb.base.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.ExecutionException;


import static org.junit.Assert.assertEquals;

public class VmlensMultithreadTest {
    static DBMS db;
    static TxMgr txMgr;
    static Map<Object, Object> objectMap;
    private final static Logger logger = LogManager.getLogger(VmlensMultithreadTest.class);

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

    @BeforeClass
    public static void before() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        db = OATSDBType.dbmsFactory(OATSDBType.V1);
        txMgr = OATSDBType.txMgrFactory(OATSDBType.V1);
        txMgr.begin() ;
        objectMap = db.createMap("obj", Object.class, Object.class);
        txMgr.commit();

    }

    @Test
    public void simpleTwoThreadConcurrentPut() throws ExecutionException, InterruptedException, SystemException, NotSupportedException, RollbackException {
        try (AllInterleavings allInterleavings = new AllInterleavings("2ConcurrentPut");){
            while(allInterleavings.hasNext()){
                Thread first = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.put("First", "Set");
                        txMgr.commit();
                    } catch (Exception e){ e.printStackTrace(); };
                });
                Thread second = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.put("Second", "Set");
                        txMgr.commit();
                    } catch (Exception e){ e.printStackTrace(); };
                });
                // Wait until all threads are finish
                first.start();
                second.start();
                first.join();
                second.join();

                txMgr.begin();
                objectMap.remove("First");
                objectMap.remove("Second");

            }
        }
    }

    @Test
    public void simpleTwoThreadConcurrentGet() throws ExecutionException, InterruptedException, SystemException, NotSupportedException, RollbackException {
        try (AllInterleavings allInterleavings = new AllInterleavings("2ConcurrentGet");){
            while(allInterleavings.hasNext()){
                Thread first = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.get("First");
                        txMgr.commit();
                    } catch (Exception e){ e.printStackTrace(); };
                });
                Thread second = new Thread( () -> {
                    try {
                        txMgr.begin();
                        objectMap.get("Second");
                        txMgr.commit();
                    } catch (Exception e){ e.printStackTrace(); };
                });
                // Wait until all threads are finish
                first.start();
                second.start();
                first.join();
                second.join();

            }
        }
    }

}
