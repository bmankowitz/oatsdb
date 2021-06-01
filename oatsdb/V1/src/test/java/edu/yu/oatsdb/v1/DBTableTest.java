package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class DBTableTest {
    DBMS db;
    TxMgr txMgr;
    Map<Character, String> gradeDetail;
    int i;

    public DBTableTest() throws InstantiationException {
        //DBMS db = OATSDBType.dbmsFactory(OATSDBType.v1);
        //TxMgr txMgr = OATSDBType.txMgrFactory(OATSDBType.v1);
        int i = 1;
    }

    @Before
    public void before() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        db = OATSDBType.dbmsFactory(OATSDBType.V1);
        txMgr = OATSDBType.txMgrFactory(OATSDBType.V1);
        txMgr.begin();
        gradeDetail = db.createMap("grades", Character.class, String.class);
        gradeDetail.put('A', "Best Grade");
        gradeDetail.put('B', "OK Grade");
        gradeDetail.put('C', "Average");
        txMgr.commit();
    }

    @After
    public void after() throws InstantiationException {
        //db = null;
        //txMgr = null;
    }


    @Test(expected = ClientNotInTxException.class)
    public void putOutsideTx() throws InstantiationException, SystemException, NotSupportedException {
        gradeDetail.put('D', "ERROR");
    }
    @Test
    public void putOutsideTxIntegrity() throws InstantiationException, SystemException, NotSupportedException {
        try {
            gradeDetail.put('D', "ERROR");
        }catch (ClientNotInTxException e){
            txMgr.begin();
            System.out.println(gradeDetail.get('D'));
            assertNull(gradeDetail.get('D'));
            txMgr.rollback();
        }
    }
    @Test(expected = ClientNotInTxException.class)
    public void getOutsideTx() throws InstantiationException, SystemException, NotSupportedException {
        gradeDetail.get('A');
    }
    @Test
    public void getElementAfterCommit() throws InstantiationException, SystemException, NotSupportedException {
        txMgr.begin();
        gradeDetail.get('A');
        txMgr.rollback();
    }
    @Test
    public void getElementAfterRollback() throws InstantiationException, SystemException, NotSupportedException {
        txMgr.begin();
        gradeDetail.put('E', "Failed Tx");
        assertEquals("Failed Tx", gradeDetail.get('E'));
        txMgr.rollback();
        txMgr.begin();
        assertNull(gradeDetail.get('E'));
        txMgr.rollback();
    }
    @Test
    public void referenceVsDatabaseIntegrity() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        gradeDetail.put('E', "OtherStuff");
        txMgr.commit();
        txMgr.begin();
        assertEquals(gradeDetail.get('E'),db.getMap("grades", Character.class, String.class).get('E'));
        txMgr.rollback();
    }
    @Test(expected = IllegalArgumentException.class)
    public void mapKeyClassNull() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Object, Integer> nullKey;
        nullKey = db.createMap("nullKeyClass", null, Integer.class);
        txMgr.commit();
    }
    @Test(expected = IllegalArgumentException.class)
    public void mapValueClassNull() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Integer, Object> nullValue;
        nullValue = db.createMap("nullValueClass", Integer.class, null);
        txMgr.commit();
    }
    @Test(expected = IllegalArgumentException.class)
    public void mapKeyNull() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Integer, Character> nullKey;
        nullKey = db.createMap("nullKey", Integer.class, Character.class);
        nullKey.put(null, 'c');
        txMgr.commit();
    }
    @Test
    public void mapValueNull() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Map<Integer, Character> nullValue;
        nullValue = db.createMap("nullKey", Integer.class, Character.class);
        nullValue.put(3, null);
        txMgr.commit();
    }
    @Test
    public void getMapTwice() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        db.getMap("grades", Character.class, String.class);
        db.getMap("grades", Character.class, String.class);
        txMgr.commit();
    }
    @Test
    public void getKeyTwice() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        gradeDetail.get('A');
        gradeDetail.get('A');
        txMgr.commit();
    }

}
