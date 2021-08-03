package edu.yu.oatsdb.v1;

import edu.yu.oatsdb.base.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.util.ArrayList;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;

public class DBTableTest {
    DBMS db;
    TxMgr txMgr;
    Map<Character, String> gradeDetail;
    int i;


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
        db = null;
        txMgr = null;
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
    public void removeReturnsElement() throws InstantiationException, SystemException, NotSupportedException {
        txMgr.begin();
        gradeDetail.put(']', "Set");
        assertEquals("Set", gradeDetail.remove(']'));
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
    public void mapReferenceVsDatabaseIntegrity() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        gradeDetail.put('E', "OtherStuff");
        txMgr.commit();
        txMgr.begin();
        assertEquals(gradeDetail.get('E'),db.getMap("grades", Character.class, String.class).get('E'));
        txMgr.rollback();
    }
    @Test
    public void mapEntryReferenceIsStale() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        ArrayList<String> arr = new ArrayList<>();
        arr.add("elem1");
        Map<Character, ArrayList> map = db.createMap("arr", Character.class, ArrayList.class);
        map.put('A', arr);
        txMgr.commit();
        arr.add("elem2");
        txMgr.begin();
        assertFalse(map.get('A').contains("elem2"));
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
    @Test
    public void getCachesOldValue() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        gradeDetail.put('3', "FirstValue");
        txMgr.commit();
        txMgr.begin();
        //now for the real test
        assertEquals(gradeDetail.get('3'), "FirstValue");
        gradeDetail.put('3', "SecondValue");
        assertEquals(gradeDetail.get('3'), "SecondValue");
        txMgr.rollback();
        txMgr.begin();
        assertEquals(gradeDetail.get('3'), "FirstValue");
        txMgr.commit();

    }

    @Test
    public void putIsCached() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        gradeDetail.put('9', "Hello");
        assertEquals("Hello", gradeDetail.get('9'));
        txMgr.commit();
        txMgr.begin();
        assertEquals("Hello", gradeDetail.get('9'));
        txMgr.commit();
    }

    @Test
    public void removeIsCached() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        gradeDetail.put('9', "Hello");
        txMgr.commit();
        txMgr.begin();
        assertEquals("Hello", gradeDetail.get('9'));
        assertEquals(gradeDetail.remove('9'), "Hello");
        assertNull(gradeDetail.get('9'));
        txMgr.commit();
        txMgr.begin();
        assertNull(gradeDetail.get('9'));
        txMgr.commit();
    }
    @Test
    public void removeRollbackIsNotSaved() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        gradeDetail.put('9', "Hello");
        txMgr.commit();
        txMgr.begin();
        assertEquals("Hello", gradeDetail.get('9'));
        assertEquals("Hello", gradeDetail.remove('9'));
        assertNull(gradeDetail.get('9'));
        txMgr.rollback();
        txMgr.begin();
        assertEquals("Hello", gradeDetail.get('9'));
        txMgr.commit();
    }

}
