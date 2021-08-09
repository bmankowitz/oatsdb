package edu.yu.oatsdb.v2;

import edu.yu.oatsdb.base.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class TxMgrImplTest {

    DBMS db;
    TxMgr txMgr;
    int i;

    public TxMgrImplTest() throws InstantiationException {
        //DBMS db = OATSDBType.dbmsFactory(OATSDBType.v1);
        //TxMgr txMgr = OATSDBType.txMgrFactory(OATSDBType.v1);
        int i = 1;
    }

    @Before
    public void before() throws InstantiationException {
        db = OATSDBType.dbmsFactory(OATSDBType.V2);
        txMgr = OATSDBType.txMgrFactory(OATSDBType.V2);
    }
    @After
    public void after() throws InstantiationException {
        //db = null;
        //txMgr = null;
    }

    //EXPECTED OPERATIONS todo:add more

    //EXCEPTIONS:
    @Test(expected = IllegalStateException.class)
    public void commitAfterCommitThrowException() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        txMgr.commit();
        txMgr.commit();
    }
    @Test
    public void commitAfterCommitIntegrityTest() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        Map<Character, String> gradeDetail = null;
        try {
            txMgr.begin();
            gradeDetail = db.createMap("grades", Character.class, String.class);
            gradeDetail.put('i', "stringtest");
            txMgr.commit();
            txMgr.commit();
        } catch(IllegalStateException e){
            txMgr.begin();
            assertEquals("stringtest", db.getMap("grades", Character.class, String.class).get('i'));
            txMgr.rollback();
        }
    }
    @Test(expected = IllegalStateException.class)
    public void commitAfterRollbackElementThrowException() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        txMgr.rollback();
        txMgr.commit();
    }
    @Test
    public void commitAfterRollbackElementIntegrityTest() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        Map<Character, String> gradeDetail = null;
        try {
            txMgr.begin();
            gradeDetail = db.createMap("grades", Character.class, String.class);
            txMgr.commit();
            txMgr.begin();
            gradeDetail.put('i', "stringtest");
            txMgr.rollback();
            txMgr.commit();
        } catch(IllegalStateException e){
            txMgr.begin();
            assertNull(db.getMap("grades", Character.class, String.class).get('i'));
            txMgr.rollback();
        }
    }
    @Test()
    public void commitAfterRollbackTableIntegrityTest() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        Map<Character, String> gradeDetail = null;
        try {
            txMgr.begin();
            gradeDetail = db.createMap("grades", Character.class, String.class);
            txMgr.rollback();
            txMgr.commit();
        } catch(IllegalStateException e){
            txMgr.begin();
            //expect that this map does exist
            //NOTE: This is allowed per Piazza
            assertEquals(gradeDetail, db.getMap("grades", Character.class, String.class));
            txMgr.rollback();
        }
    }
    @Test(expected = IllegalStateException.class)
    public void commitOutsideTx() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.commit();
    }
    @Test(expected = IllegalStateException.class)
    public void rollbackOutsideTx() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.rollback();
    }
    @Test(expected = NotSupportedException.class)
    public void beginAfterBegin() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        txMgr.begin();
    }
    @Test
    public void standardCommit() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades",Character.class, String.class);
        gradeDetail.put('@',"sa");
        gradeDetail.put('4',"sb");
        txMgr.commit();
        txMgr.begin();
        assertEquals("sa", gradeDetail.get('@'));
        assertEquals("sb", db.getMap("grades",Character.class, String.class).get('4'));
    }
    @Test
    public void txStatusActiveBeforeCommit() throws SystemException, NotSupportedException, RollbackException, InterruptedException {
        txMgr.begin();
        assertEquals(TxStatus.ACTIVE, txMgr.getStatus());
        txMgr.commit();
    }
    @Test
    public void txStatusNoTxAfterCommit() throws SystemException, NotSupportedException, RollbackException, InterruptedException {
        txMgr.begin();
        assertEquals(TxStatus.ACTIVE, txMgr.getStatus());
        txMgr.commit();
        Thread.sleep(100);
        assertEquals(TxStatus.NO_TRANSACTION,txMgr.getStatus());
    }
    @Test
    public void txStatusNoTxAfterRollback() throws SystemException, NotSupportedException, RollbackException, InterruptedException {
        txMgr.begin();
        assertEquals(TxStatus.ACTIVE, txMgr.getStatus());
        txMgr.rollback();
        Thread.sleep(100);
        assertEquals(TxStatus.NO_TRANSACTION,txMgr.getStatus());
    }
    @Test
    public void txCompletionStatusCommittedAfterCommit() throws SystemException, NotSupportedException, RollbackException, InterruptedException {
        txMgr.begin();
        assertEquals(TxStatus.ACTIVE, txMgr.getStatus());
        TxImpl tx = (TxImpl) txMgr.getTx();
        txMgr.commit();
        Thread.sleep(100);
        assertEquals(tx.getCompletionStatus(), TxCompletionStatus.COMMITTED);
    }
    @Test
    public void txCompletionStatusRolledBackAfterRollback() throws SystemException, NotSupportedException, RollbackException, InterruptedException {
        txMgr.begin();
        assertEquals(TxStatus.ACTIVE, txMgr.getStatus());
        TxImpl tx = (TxImpl) txMgr.getTx();
        txMgr.rollback();
        Thread.sleep(100);
        assertEquals(tx.getCompletionStatus(), TxCompletionStatus.ROLLEDBACK);
    }

    @Deprecated @Test
    public void copyTest(){
        java.util.HashMap<Integer, Character> hash1 = new java.util.HashMap<Integer, Character>();
        hash1.put(1, 'a');
        hash1.put(2, 'b');
        java.util.HashMap<Integer, Character> hash2 = new java.util.HashMap<Integer, Character>(hash1);
        hash1.put(1, 'x');
        System.out.println(hash1);
        System.out.println(hash2);
    }
    @Deprecated @Test
    public void timeTest(){
        java.util.HashMap<Integer, Double> h = new java.util.HashMap<>();
        for(int i = 0; i < 10000; i+= Math.random()*300){
            h.put(i, 1.0);
        }
        long startTime = System.nanoTime();
        for(int i = 0; i < 10000; i++){
            h.put(i, 1.0);
        }


//        for(int i = 0; i < 10000000; i++){
//            if(!h.containsKey(i)){
//                h.put(i, 1.0);
//            }
//        }
        long endTime = System.nanoTime();

        long duration = (endTime - startTime);  //divide by 1000000 to get milliseconds.
        System.out.println(duration);


    }


}