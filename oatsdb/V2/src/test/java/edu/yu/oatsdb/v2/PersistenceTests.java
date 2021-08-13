package edu.yu.oatsdb.v2;

import edu.yu.oatsdb.base.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class PersistenceTests {
    ConfigurablePersistentDBMS db;
    TxMgr txMgr;
    Map<Character, String> gradeDetail;
    int i;


    @Before
    public void before() throws InstantiationException, SystemException, NotSupportedException, RollbackException {
        db = (ConfigurablePersistentDBMS) OATSDBType.dbmsFactory(OATSDBType.V2);
        txMgr = OATSDBType.txMgrFactory(OATSDBType.V2);
    }

    @After
    public void after() throws InstantiationException {
        db = null;
        txMgr = null;
    }

    @Test
    @Deprecated
    public void simpleMasterTest() throws SystemException {
        HashMap<String, Long> map = new HashMap<>();
        map.put("tet", 23L);
//        Globals.setMasterValues(map);
    }

    @Test
    @Deprecated
    public void simpleWriteTempTest() throws SystemException {
        HashMap<String, Integer> map = new HashMap<>();
        map.put("tet", 1);
        map.put("ftet", 3);
        DBTableMetadata<String, Integer> metaMap =
                new DBTableMetadata<>("name", String.class, Integer.class, map, 2, 23);
        Globals.writeTempMap(metaMap);
    }

    @Test
    public void simpleGetStorageTest() throws SystemException {
        System.out.println(db.getDiskUsageInMB());
    }

    @Test
    public void simpleClearStorageTest() throws SystemException {
        db.clear();
        Assert.assertEquals(0, Globals.getStorageSize());
    }

    @Test
    public void simpleCommitTest() throws InstantiationException, SystemException, NotSupportedException,
            RollbackException {
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades", Character.class, String.class);
        gradeDetail.put('s', "this should persist");
        txMgr.commit();
    }

    @Test
    public void simpleReadStorageTest() throws InstantiationException, SystemException, NotSupportedException,
            RollbackException {
        Globals.readStorage();
        txMgr.begin();
        Assert.assertNotNull(db.getMap("obj", Character.class, String.class));
        txMgr.commit();
    }

    @Test
    public void generalReadWriteTest1() throws SystemException, NotSupportedException, RollbackException {
        db.clear();
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades", Character.class, String.class);
        Map<Integer, String> gradeDetail2 = db.createMap("grades2", Integer.class, String.class);
        Map<String, Double> master = db.createMap("master", String.class, Double.class);
        gradeDetail.put('s', "Set");
        gradeDetail2.put(45, "Set");
        master.put("s", 2.32);
        txMgr.commit();
        txMgr.begin();
        Map<Character, String> grade3 = db.createMap("grades3", Character.class, String.class);
        grade3.put('l', "SET");
        master.put("s", 2.32);
        txMgr.commit();

    }

    @Test
    public void generalReadWriteTest2() throws SystemException, NotSupportedException, RollbackException {
        Globals.readStorage();
        txMgr.begin();
        Assert.assertEquals("Set", db.getMap("grades", Character.class, String.class).get('s'));
        Assert.assertEquals("Set", db.getMap("grades2", Integer.class, String.class).get(45));
        Assert.assertEquals("SET", db.getMap("grades3", Character.class, String.class).get('l'));
        Assert.assertEquals((Double) 2.32, db.getMap("master", String.class, Double.class).get("s"));
        txMgr.commit();
    }

    @Test
    public void crashRecovery1() throws SystemException, NotSupportedException, RollbackException {
        db.clear();
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("grades", Character.class, String.class);
        Map<Integer, String> gradeDetail2 = db.createMap("grades2", Integer.class, String.class);
        Map<String, Double> master = db.createMap("master", String.class, Double.class);
        gradeDetail.put('s', "Set");
        gradeDetail2.put(45, "Set");
        master.put("s", 2.32);
        txMgr.commit();
        txMgr.begin();
        Map<Character, String> grade3 = db.createMap("grades3", Character.class, String.class);
        grade3.put('l', "SET");
        master.put("s", 2.32);
        txMgr.commit();

    }

    @Test
    public void crashRecovery2() throws SystemException, NotSupportedException, RollbackException {
        Globals.readStorage();
        txMgr.begin();
        Assert.assertEquals("Set", db.getMap("grades", Character.class, String.class).get('s'));
        Assert.assertEquals("Set", db.getMap("grades2", Integer.class, String.class).get(45));
        Assert.assertEquals("SET", db.getMap("grades3", Character.class, String.class).get('l'));
        Assert.assertEquals((Double) 2.32, db.getMap("master", String.class, Double.class).get("s"));
        txMgr.commit();
    }

    @Test
    public void crashRecoveryA() throws SystemException, NotSupportedException, RollbackException {
        db.clear();
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("justmap", Character.class, String.class);
        txMgr.commit();

    }

    @Test
    public void crashRecoveryB() throws SystemException, NotSupportedException, RollbackException {
        //Globals.readStorage();
        txMgr.begin();
        Map<Character, String> gradeDetail = db.getMap("justmap", Character.class, String.class);
        txMgr.commit();
    }

    @Test
    public void deletedValuesStayDeleted1() throws SystemException, NotSupportedException, RollbackException {
        db.clear();
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("justmap", Character.class, String.class);
        gradeDetail.put('a', "Set 1");
        txMgr.commit();
        txMgr.begin();
        gradeDetail.remove('a', "Set 1");
        txMgr.commit();


    }

    @Test
    public void deletedValuesStayDeleted2() throws SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Assert.assertNull(db.getMap("justmap", Character.class, String.class).get('a'));
        txMgr.commit();

    }
    @Test
    public void putValuesStayPut1() throws SystemException, NotSupportedException, RollbackException {
        db.clear();
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("justmap", Character.class, String.class);
        gradeDetail.put('a', "Set 1");
        txMgr.commit();
        txMgr.begin();
        gradeDetail.put('a', "Set 2");
        txMgr.commit();


    }

    @Test
    public void putValuesStayPut2() throws SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Assert.assertEquals("Set 2", db.getMap("justmap", Character.class, String.class).get('a'));
        txMgr.commit();

    }
    @Test
    public void getValuesNotModified1() throws SystemException, NotSupportedException, RollbackException {
        db.clear();
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("justmap", Character.class, String.class);
        gradeDetail.put('a', "Set 1");
        txMgr.commit();
        txMgr.begin();
        gradeDetail.get('x');
        txMgr.commit();


    }

    @Test
    public void getValuesNotModified2() throws SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Assert.assertNull(db.getMap("justmap", Character.class, String.class).get('x'));
        txMgr.commit();

    }
    @Test
    public void multipleOperations1() throws SystemException, NotSupportedException, RollbackException {
        db.clear();
        txMgr.begin();
        Map<Character, String> gradeDetail = db.createMap("justmap", Character.class, String.class);
        gradeDetail.put('a', "Set 1");
        txMgr.commit();
        txMgr.begin();
        gradeDetail.get('x');
        Map<Character, String> gradeDetail2 = db.createMap("justmasp", Character.class, String.class);
        Assert.assertNull(gradeDetail.remove('3'));
        txMgr.commit();
        txMgr.begin();
        txMgr.commit();
        txMgr.begin();
        gradeDetail.put('2', "h");
        gradeDetail.put('2', "h");
        gradeDetail.put('7', "SET");
        txMgr.commit();

        txMgr.begin();
        gradeDetail2.put('2', "h");
        gradeDetail2.put('2', "h");
        gradeDetail2.put('7', "SET");
        gradeDetail.remove('a');
        txMgr.rollback();



    }

    @Test
    public void multipleOperations2() throws SystemException, NotSupportedException, RollbackException {
        txMgr.begin();
        Assert.assertNull(db.getMap("justmap", Character.class, String.class).get('x'));
        Assert.assertEquals("h",db.getMap("justmap", Character.class, String.class).get('2'));
        Assert.assertEquals("SET",db.getMap("justmap", Character.class, String.class).get('7'));
        Assert.assertEquals("Set 1",db.getMap("justmap", Character.class, String.class).get('a'));
        Assert.assertNull(db.getMap("justmasp", Character.class, String.class).get('2'));
        Assert.assertNull(db.getMap("justmasp", Character.class, String.class).get('3'));
        Assert.assertNull(db.getMap("justmasp", Character.class, String.class).get('7'));
        txMgr.commit();

    }
}
