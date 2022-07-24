package edu.berkeley.cs186.database.index;

import edu.berkeley.cs186.database.TimeoutScaling;
import edu.berkeley.cs186.database.categories.HiddenTests;
import edu.berkeley.cs186.database.categories.Proj2Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.categories.SystemTests;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.concurrency.LockContext;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import edu.berkeley.cs186.database.databox.Type;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.io.MemoryDiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.ClockEvictionPolicy;
import edu.berkeley.cs186.database.recovery.DummyRecoveryManager;
import edu.berkeley.cs186.database.table.RecordId;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.DisableOnDebug;
import org.junit.rules.TestRule;
import org.junit.rules.Timeout;

import java.util.*;

import static org.junit.Assert.*;

@Category(Proj2Tests.class)
public class TestInnerNode {
    private BufferManager bufferManager;
    private BPlusTreeMetadata metadata;
    private LockContext treeContext;

    // 1 second max per method tested.
    @Rule
    public TestRule globalTimeout = new DisableOnDebug(Timeout.millis((long) (
            1000 * TimeoutScaling.factor)));

    // inner, leaf0, leaf1, and leaf2 collectively form the following B+ tree:
    //
    //                               inner
    //                               +----+----+----+----+
    //                               | 10 | 20 |    |    |
    //                               +----+----+----+----+
    //                              /     |     \
    //                         ____/      |      \____
    //                        /           |           \
    //   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
    //   |  1 |  2 |  3 |    |  | 11 | 12 | 13 |    |  | 21 | 22 | 23 |    |
    //   +----+----+----+----+  +----+----+----+----+  +----+----+----+----+
    //   leaf0                  leaf1                  leaf2
    //
    // innerKeys, innerChildren, keys0, rids0, keys1, rids1, keys2, and rids2
    // hold *copies* of the contents of the nodes. To test out a certain method
    // of a tree---for example, put---we can issue a put against the tree,
    // update one of innerKeys, innerChildren, keys{0,1,2}, or rids{0,1,2}, and
    // then check that the contents of the tree match our expectations. For
    // example:
    //
    //   IntDataBox key = new IntDataBox(4);
    //   RecordId rid = new RecordId(4, (short) 4);
    //   inner.put(key, rid);
    //
    //   // (4, (4, 4)) is added to leaf 0, so we update keys0 and rids0 and
    //   // check that it matches the contents of leaf0.
    //   keys0.add(key);
    //   rids0.add(rid);
    //   assertEquals(keys0, getLeaf(leaf0).getKeys());
    //   assertEquals(rids0, getLeaf(leaf0).getRids());
    //
    //   // Leaf 1 should be unchanged which we can check:
    //   assertEquals(keys1, getLeaf(leaf1).getKeys());
    //   assertEquals(rids1, getLeaf(leaf1).getRids());
    //
    //   // Writing all these assertEquals is boilerplate, so we can abstract
    //   // it in checkTreeMatchesExpectations().
    //   checkTreeMatchesExpectations();
    //
    // Note that we cannot simply store the LeafNodes as members because when
    // we call something like inner.put(k), the inner node constructs a new
    // LeafNode from the serialization and forwards the put to that. It would
    // not affect our the in-memory values of our members. Also note that all
    // of these members are initialized by resetMembers before every test case
    // is run.

    private List<DataBox> innerKeys;
    private List<Long> innerChildren;
    private InnerNode inner;
    private List<DataBox> keys0;
    private List<RecordId> rids0;
    private long leaf0;
    private List<DataBox> keys1;
    private List<RecordId> rids1;
    private long leaf1;
    private List<DataBox> keys2;
    private List<RecordId> rids2;
    private long leaf2;

    // See comment above.
    @Before
    public void resetMembers() {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        this.bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        this.treeContext = new DummyLockContext();
        setBPlusTreeMetadata(Type.intType(), 2);

        // Leaf 2
        List<DataBox> keys2 = new ArrayList<>();
        keys2.add(new IntDataBox(21));
        keys2.add(new IntDataBox(22));
        keys2.add(new IntDataBox(23));
        List<RecordId> rids2 = new ArrayList<>();
        rids2.add(new RecordId(21, (short) 21));
        rids2.add(new RecordId(22, (short) 22));
        rids2.add(new RecordId(23, (short) 23));
        Optional<Long> sibling2 = Optional.empty();
        LeafNode leaf2 = new LeafNode(metadata, bufferManager, keys2, rids2, sibling2, treeContext);

        this.keys2 = new ArrayList<>(keys2);
        this.rids2 = new ArrayList<>(rids2);
        this.leaf2 = leaf2.getPage().getPageNum();

        // Leaf 1
        keys1 = new ArrayList<>();
        keys1.add(new IntDataBox(11));
        keys1.add(new IntDataBox(12));
        keys1.add(new IntDataBox(13));
        rids1 = new ArrayList<>();
        rids1.add(new RecordId(11, (short) 11));
        rids1.add(new RecordId(12, (short) 12));
        rids1.add(new RecordId(13, (short) 13));
        Optional<Long> sibling1 = Optional.of(leaf2.getPage().getPageNum());
        LeafNode leaf1 = new LeafNode(metadata, bufferManager, keys1, rids1, sibling1, treeContext);

        this.keys1 = new ArrayList<>(keys1);
        this.rids1 = new ArrayList<>(rids1);
        this.leaf1 = leaf1.getPage().getPageNum();

        // Leaf 0
        List<DataBox> keys0 = new ArrayList<>();
        keys0.add(new IntDataBox(1));
        keys0.add(new IntDataBox(2));
        keys0.add(new IntDataBox(3));
        List<RecordId> rids0 = new ArrayList<>();
        rids0.add(new RecordId(1, (short) 1));
        rids0.add(new RecordId(2, (short) 2));
        rids0.add(new RecordId(3, (short) 3));
        Optional<Long> sibling0 = Optional.of(leaf1.getPage().getPageNum());
        LeafNode leaf0 = new LeafNode(metadata, bufferManager, keys0, rids0, sibling0, treeContext);
        this.keys0 = new ArrayList<>(keys0);
        this.rids0 = new ArrayList<>(rids0);
        this.leaf0 = leaf0.getPage().getPageNum();

        // Inner node
        List<DataBox> innerKeys = new ArrayList<>();
        innerKeys.add(new IntDataBox(10));
        innerKeys.add(new IntDataBox(20));

        List<Long> innerChildren = new ArrayList<>();
        innerChildren.add(this.leaf0);
        innerChildren.add(this.leaf1);
        innerChildren.add(this.leaf2);

        this.innerKeys = new ArrayList<>(innerKeys);
        this.innerChildren = new ArrayList<>(innerChildren);
        this.inner = new InnerNode(metadata, bufferManager, innerKeys, innerChildren, treeContext);
    }

    @After
    public void cleanup() {
        this.bufferManager.close();
    }

    private void setBPlusTreeMetadata(Type keySchema, int order) {
        this.metadata = new BPlusTreeMetadata("test", "col", keySchema, order,
                0, DiskSpaceManager.INVALID_PAGE_NUM, -1);
    }

    // See comment above.
    private LeafNode getLeaf(long pageNum) {
        return LeafNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
    }

    // See comment above.
    private void checkTreeMatchesExpectations() {
        LeafNode leaf0 = getLeaf(this.leaf0);
        LeafNode leaf1 = getLeaf(this.leaf1);
        LeafNode leaf2 = getLeaf(this.leaf2);

        assertEquals(keys0, leaf0.getKeys());
        assertEquals(rids0, leaf0.getRids());
        assertEquals(keys1, leaf1.getKeys());
        assertEquals(rids1, leaf1.getRids());
        assertEquals(keys2, leaf2.getKeys());
        assertEquals(rids2, leaf2.getRids());
        assertEquals(innerKeys, inner.getKeys());
        assertEquals(innerChildren, inner.getChildren());
    }

    // Tests ///////////////////////////////////////////////////////////////////
    @Test
    @Category(PublicTests.class)
    public void testGet() {
        LeafNode leaf0 = getLeaf(this.leaf0);
        assertNotNull(leaf0);
        for (int i = 0; i < 10; ++i) {
            assertEquals(leaf0, inner.get(new IntDataBox(i)));
        }

        LeafNode leaf1 = getLeaf(this.leaf1);
        for (int i = 10; i < 20; ++i) {
            assertEquals(leaf1, inner.get(new IntDataBox(i)));
        }

        LeafNode leaf2 = getLeaf(this.leaf2);
        for (int i = 20; i < 30; ++i) {
            assertEquals(leaf2, inner.get(new IntDataBox(i)));
        }
    }

    @Test
    @Category(PublicTests.class)
    public void testGetLeftmostLeaf() {
        assertNotNull(getLeaf(leaf0));
        assertEquals(getLeaf(leaf0), inner.getLeftmostLeaf());
    }

    @Test
    @Category(PublicTests.class)
    public void testNoOverflowPuts() {
        IntDataBox key = null;
        RecordId rid = null;

        // Add to leaf 0.
        key = new IntDataBox(0);
        rid = new RecordId(0, (short) 0);
        assertEquals(Optional.empty(), inner.put(key, rid));
        keys0.add(0, key);
        rids0.add(0, rid);
        checkTreeMatchesExpectations();

        // Add to leaf 1.
        key = new IntDataBox(14);
        rid = new RecordId(14, (short) 14);
        assertEquals(Optional.empty(), inner.put(key, rid));
        keys1.add(3, key);
        rids1.add(3, rid);
        checkTreeMatchesExpectations();

        // Add to leaf 2.
        key = new IntDataBox(20);
        rid = new RecordId(20, (short) 20);
        assertEquals(Optional.empty(), inner.put(key, rid));
        keys2.add(0, key);
        rids2.add(0, rid);
        checkTreeMatchesExpectations();
    }

    @Test
    @Category(PublicTests.class)
    public void testLeafOverflowPuts() {
        IntDataBox key = null;
        RecordId rid = null;

        // Add to leaf 0.
        key = new IntDataBox(7);
        rid = new RecordId(7, (short) 7);
        assertEquals(Optional.empty(), inner.put(key, rid));
        keys0.add(key);
        rids0.add(rid);
        checkTreeMatchesExpectations();

        // Add to leaf 0, then leaf 0 will split.
        key = new IntDataBox(5);
        rid = new RecordId(5, (short) 5);
        Optional<Pair<DataBox, Long>> splitKeyAndNewSplitPageNum = inner.put(key, rid);
        assert !splitKeyAndNewSplitPageNum.isPresent();
        innerKeys.add(0, new IntDataBox(3));
        assertEquals(innerKeys, inner.getKeys());
        keys0 = keys0.subList(0, 2);
        rids0 = rids0.subList(0, 2);
        LeafNode leaf0Node = LeafNode.fromBytes(metadata, bufferManager, treeContext, inner.getChildren().get(0));
        assertEquals(keys0, leaf0Node.getKeys());
        assertEquals(rids0, leaf0Node.getRids());

        LeafNode addLeafNode = LeafNode.fromBytes(metadata, bufferManager, treeContext, inner.getChildren().get(1));
        List<DataBox> addLeafNodeKeys = Arrays.asList(new IntDataBox(3), new IntDataBox(5), new IntDataBox(7));
        List<RecordId> addLeafNodeRids = Arrays.asList(new RecordId(3, (short) 3), new RecordId(5, (short) 5), new RecordId(7, (short) 7));
        assertEquals(addLeafNodeKeys, addLeafNode.getKeys());
        assertEquals(addLeafNodeRids, addLeafNode.getRids());
    }

    @Test
    @Category(PublicTests.class)
    public void testInnerOverflowPuts() {
        IntDataBox key = null;
        RecordId rid = null;
        Optional<Pair<DataBox, Long>> splitKeyAndNewSplitPageNum;

        // Add to leaf 0.
        key = new IntDataBox(7);
        rid = new RecordId(7, (short) 7);
        splitKeyAndNewSplitPageNum = inner.put(key, rid);
        assert !splitKeyAndNewSplitPageNum.isPresent();

        // Add to leaf 0, then leaf 0 will split;
        // leaf0(1,2,3,7)->leaf0(1,2)+leaf0Split(3,5,7).
        // inner(10,20)->inner(3,10,20)
        key = new IntDataBox(5);
        rid = new RecordId(5, (short) 5);
        splitKeyAndNewSplitPageNum = inner.put(key, rid);
        assert !splitKeyAndNewSplitPageNum.isPresent();
        innerKeys.add(0, new IntDataBox(3));
        assertEquals(innerKeys, inner.getKeys());

        // Add to leaf1.
        key = new IntDataBox(18);
        rid = new RecordId(18, (short) 18);
        splitKeyAndNewSplitPageNum = inner.put(key, rid);
        assert !splitKeyAndNewSplitPageNum.isPresent();

        // Add to leaf1, then leaf1 will split;
        // leaf1(11,12,13,18)->leaf1(11,12)+leaf1Split(13,15,18).
        // inner(3,10,20)->inner(3,10,13,20)
        key = new IntDataBox(15);
        rid = new RecordId(15, (short) 15);
        splitKeyAndNewSplitPageNum = inner.put(key, rid);
        assert !splitKeyAndNewSplitPageNum.isPresent();
        innerKeys.add(2, new IntDataBox(13));
        assertEquals(innerKeys, inner.getKeys());

        // Add to leaf2.
        key = new IntDataBox(28);
        rid = new RecordId(28, (short) 28);
        splitKeyAndNewSplitPageNum = inner.put(key, rid);
        assert !splitKeyAndNewSplitPageNum.isPresent();

        // Add to leaf2, then leaf2 will split;
        // leaf2(21,22,23,28)->leaf2(21,22)+leaf2Split(23,25,28).
        // inner(3,10,13,20)->inner(3,10)+innerSplit(20,23)
        key = new IntDataBox(25);
        rid = new RecordId(25, (short) 25);
        splitKeyAndNewSplitPageNum = inner.put(key, rid);
        assert splitKeyAndNewSplitPageNum.isPresent();
        DataBox splitKey = splitKeyAndNewSplitPageNum.get().getFirst();
        Long addSplitPageNum = splitKeyAndNewSplitPageNum.get().getSecond();
        assert splitKey.getInt() == 13;
        innerKeys = innerKeys.subList(0, 2);
        assertEquals(innerKeys, inner.getKeys());
        InnerNode addInnerNode = InnerNode.fromBytes(metadata, bufferManager, treeContext, addSplitPageNum);
        assertEquals(Arrays.asList(new IntDataBox(20), new IntDataBox(23)), addInnerNode.getKeys());


        assert inner.getChildren().size() == 3;
        assert addInnerNode.getChildren().size() == 3;

        assertInnerChildrenEquals(inner, 0, 1, 2);
        assertInnerChildrenEquals(inner, 1, 3, 5, 7);
        assertInnerChildrenEquals(inner, 2, 11, 12);
        assertInnerChildrenEquals(addInnerNode, 0, 13, 15, 18);
        assertInnerChildrenEquals(addInnerNode, 1, 21, 22);
        assertInnerChildrenEquals(addInnerNode, 2, 23, 25, 28);

    }

    private void assertInnerChildrenEquals(InnerNode innerNode, int childIndex, int... keyVals) {
        LeafNode innerChild = LeafNode.fromBytes(metadata, bufferManager, treeContext, innerNode.getChildren().get(childIndex));
        List<DataBox> innerChildKeys = new ArrayList<>();
        List<RecordId> innerChildRids = new ArrayList<>();
        for (int i = 0; i < keyVals.length; i++) {
            innerChildKeys.add(new IntDataBox(keyVals[i]));
            innerChildRids.add(new RecordId(keyVals[i], (short) keyVals[i]));
        }
        assertEquals(innerChildKeys, innerChild.getKeys());
        assertEquals(innerChildRids, innerChild.getRids());
    }

    @Test
    @Category(PublicTests.class)
    public void testRemove() {
        // Remove from leaf 0.
        inner.remove(new IntDataBox(1));
        keys0.remove(0);
        rids0.remove(0);
        checkTreeMatchesExpectations();

        inner.remove(new IntDataBox(3));
        keys0.remove(1);
        rids0.remove(1);
        checkTreeMatchesExpectations();

        inner.remove(new IntDataBox(2));
        keys0.remove(0);
        rids0.remove(0);
        checkTreeMatchesExpectations();

        // Remove from leaf 1.
        inner.remove(new IntDataBox(11));
        keys1.remove(0);
        rids1.remove(0);
        checkTreeMatchesExpectations();

        inner.remove(new IntDataBox(13));
        keys1.remove(1);
        rids1.remove(1);
        checkTreeMatchesExpectations();

        inner.remove(new IntDataBox(12));
        keys1.remove(0);
        rids1.remove(0);
        checkTreeMatchesExpectations();

        // Remove from leaf 2.
        inner.remove(new IntDataBox(23));
        keys2.remove(2);
        rids2.remove(2);
        checkTreeMatchesExpectations();

        inner.remove(new IntDataBox(22));
        keys2.remove(1);
        rids2.remove(1);
        checkTreeMatchesExpectations();

        inner.remove(new IntDataBox(21));
        keys2.remove(0);
        rids2.remove(0);
        checkTreeMatchesExpectations();
    }

    @Test
    @Category(SystemTests.class)
    public void testOverFlowBulkLoad() {
        DiskSpaceManager diskSpaceManager = new MemoryDiskSpaceManager();
        diskSpaceManager.allocPart(0);
        BufferManager bufferManager = new BufferManager(diskSpaceManager, new DummyRecoveryManager(), 1024,
                new ClockEvictionPolicy());
        DummyLockContext treeContext = new DummyLockContext();
        BPlusTreeMetadata metadata = new BPlusTreeMetadata("test_inner_bulk_node", "col", Type.intType(), 3,
                0, DiskSpaceManager.INVALID_PAGE_NUM, -1);
        float fillFactor = (float) 2 / 3;

        //leaf 2
        List<DataBox> keys2 = new ArrayList<>();
        keys2.add(new IntDataBox(5));
        keys2.add(new IntDataBox(6));
        keys2.add(new IntDataBox(7));
        List<RecordId> rids2 = new ArrayList<>();
        rids2.add(new RecordId(11111, (short) 5));
        rids2.add(new RecordId(11111, (short) 6));
        rids2.add(new RecordId(11111, (short) 7));
        Optional<Long> sibling2 = Optional.empty();
        LeafNode leaf2 = new LeafNode(metadata, bufferManager, keys2, rids2, sibling2, treeContext);

        //leaf 1
        List<DataBox> keys1 = new ArrayList<>();
        keys1.add(new IntDataBox(1));
        keys1.add(new IntDataBox(2));
        keys1.add(new IntDataBox(3));
        keys1.add(new IntDataBox(4));
        List<RecordId> rids1 = new ArrayList<>();
        rids1.add(new RecordId(11111, (short) 1));
        rids1.add(new RecordId(11111, (short) 2));
        rids1.add(new RecordId(11111, (short) 3));
        rids1.add(new RecordId(11111, (short) 4));
        Optional<Long> sibling1 = Optional.of(leaf2.getPage().getPageNum());
        LeafNode leaf1 = new LeafNode(metadata, bufferManager, keys1, rids1, sibling1, treeContext);

        // Inner node
        List<DataBox> innerKeys = new ArrayList<>();
        innerKeys.add(new IntDataBox(5));

        List<Long> innerChildren = new ArrayList<>();
        innerChildren.add(leaf1.getPage().getPageNum());
        innerChildren.add(leaf2.getPage().getPageNum());

        InnerNode innerNode = new InnerNode(metadata, bufferManager, innerKeys, innerChildren, treeContext);

        List<Pair<DataBox, RecordId>> data = new ArrayList<>();
        for (int i = 8; i < 32; i++) {
            data.add(new Pair<>(new IntDataBox(i), new RecordId(1111, (short) i)));
        }
        Optional<Pair<DataBox, Long>> splitKeyAndNewPageNumOption = innerNode.bulkLoad(data.iterator(), fillFactor);
        assert splitKeyAndNewPageNumOption.isPresent();
        DataBox splitKey = splitKeyAndNewPageNumOption.get().getFirst();
        assert splitKey.getInt() == 17;
        Long newPageNum = splitKeyAndNewPageNumOption.get().getSecond();
        BPlusNode newNode = BPlusNode.fromBytes(metadata, bufferManager, treeContext, newPageNum);
        assert newNode instanceof InnerNode;
        InnerNode newInnerNode = (InnerNode) newNode;

        int fillFactorCapacity = (int) (2 * metadata.getOrder() * fillFactor);
        //check innerNode
        for (int i = 0; i < innerNode.getKeys().size(); i++) {
            assert 1 + (i + 1) * fillFactorCapacity == innerNode.getKeys().get(i).getInt();
        }
        for (int i = 0; i < innerNode.getChildren().size(); i++) {
            LeafNode leafNode = (LeafNode) BPlusNode.fromBytes(metadata, bufferManager, treeContext, innerNode.getChildren().get(i));
            for (int j = 0; j < leafNode.getKeys().size(); j++) {
                assert (i * fillFactorCapacity + j + 1) == leafNode.getKeys().get(j).getInt();
            }
        }

        //check newInnerNode
        for (int i = 0; i < newInnerNode.getKeys().size(); i++) {
            assert 17 + (i + 1) * fillFactorCapacity == newInnerNode.getKeys().get(i).getInt();
        }
        for (int i = 0; i < newInnerNode.getChildren().size(); i++) {
            LeafNode leafNode = (LeafNode) BPlusNode.fromBytes(metadata, bufferManager, treeContext, newInnerNode.getChildren().get(i));
            for (int j = 0; j < leafNode.getKeys().size(); j++) {
                assert i * fillFactorCapacity + j + 17 == leafNode.getKeys().get(j).getInt();
            }
        }
    }

    @Test
    @Category(SystemTests.class)
    public void testMaxOrder() {
        // Note that this white box test depend critically on the implementation
        // of toBytes and includes a lot of magic numbers that won't make sense
        // unless you read toBytes.
        assertEquals(4, Type.intType().getSizeInBytes());
        assertEquals(8, Type.longType().getSizeInBytes());
        for (int d = 0; d < 10; ++d) {
            int dd = d + 1;
            for (int i = 5 + (2 * d * 4) + ((2 * d + 1) * 8); i < 5 + (2 * dd * 4) + ((2 * dd + 1) * 8); ++i) {
                assertEquals(d, InnerNode.maxOrder((short) i, Type.intType()));
            }
        }
    }

    @Test
    @Category(SystemTests.class)
    public void testnumLessThanEqual() {
        List<Integer> empty = Collections.emptyList();
        assertEquals(0, InnerNode.numLessThanEqual(0, empty));

        List<Integer> contiguous = Arrays.asList(1, 2, 3, 4, 5);
        assertEquals(0, InnerNode.numLessThanEqual(0, contiguous));
        assertEquals(1, InnerNode.numLessThanEqual(1, contiguous));
        assertEquals(2, InnerNode.numLessThanEqual(2, contiguous));
        assertEquals(3, InnerNode.numLessThanEqual(3, contiguous));
        assertEquals(4, InnerNode.numLessThanEqual(4, contiguous));
        assertEquals(5, InnerNode.numLessThanEqual(5, contiguous));
        assertEquals(5, InnerNode.numLessThanEqual(6, contiguous));
        assertEquals(5, InnerNode.numLessThanEqual(7, contiguous));

        List<Integer> sparseWithDuplicates = Arrays.asList(1, 3, 3, 3, 5);
        assertEquals(0, InnerNode.numLessThanEqual(0, sparseWithDuplicates));
        assertEquals(1, InnerNode.numLessThanEqual(1, sparseWithDuplicates));
        assertEquals(1, InnerNode.numLessThanEqual(2, sparseWithDuplicates));
        assertEquals(4, InnerNode.numLessThanEqual(3, sparseWithDuplicates));
        assertEquals(4, InnerNode.numLessThanEqual(4, sparseWithDuplicates));
        assertEquals(5, InnerNode.numLessThanEqual(5, sparseWithDuplicates));
        assertEquals(5, InnerNode.numLessThanEqual(6, sparseWithDuplicates));
        assertEquals(5, InnerNode.numLessThanEqual(7, sparseWithDuplicates));
    }

    @Test
    @Category(SystemTests.class)
    public void testnumLessThan() {
        List<Integer> empty = Collections.emptyList();
        assertEquals(0, InnerNode.numLessThanEqual(0, empty));

        List<Integer> contiguous = Arrays.asList(1, 2, 3, 4, 5);
        assertEquals(0, InnerNode.numLessThan(0, contiguous));
        assertEquals(0, InnerNode.numLessThan(1, contiguous));
        assertEquals(1, InnerNode.numLessThan(2, contiguous));
        assertEquals(2, InnerNode.numLessThan(3, contiguous));
        assertEquals(3, InnerNode.numLessThan(4, contiguous));
        assertEquals(4, InnerNode.numLessThan(5, contiguous));
        assertEquals(5, InnerNode.numLessThan(6, contiguous));
        assertEquals(5, InnerNode.numLessThan(7, contiguous));

        List<Integer> sparseWithDuplicates = Arrays.asList(1, 3, 3, 3, 5);
        assertEquals(0, InnerNode.numLessThan(0, sparseWithDuplicates));
        assertEquals(0, InnerNode.numLessThan(1, sparseWithDuplicates));
        assertEquals(1, InnerNode.numLessThan(2, sparseWithDuplicates));
        assertEquals(1, InnerNode.numLessThan(3, sparseWithDuplicates));
        assertEquals(4, InnerNode.numLessThan(4, sparseWithDuplicates));
        assertEquals(4, InnerNode.numLessThan(5, sparseWithDuplicates));
        assertEquals(5, InnerNode.numLessThan(6, sparseWithDuplicates));
        assertEquals(5, InnerNode.numLessThan(7, sparseWithDuplicates));
    }

    @Test
    @Category(PublicTests.class)
    public void testToSexp() {
        String leaf0 = "((1 (1 1)) (2 (2 2)) (3 (3 3)))";
        String leaf1 = "((11 (11 11)) (12 (12 12)) (13 (13 13)))";
        String leaf2 = "((21 (21 21)) (22 (22 22)) (23 (23 23)))";
        String expected = String.format("(%s 10 %s 20 %s)", leaf0, leaf1, leaf2);
        assertEquals(expected, inner.toSexp());
    }

    @Test
    @Category(SystemTests.class)
    public void testToAndFromBytes() {
        int d = 5;
        setBPlusTreeMetadata(Type.intType(), d);

        List<DataBox> keys = new ArrayList<>();
        List<Long> children = new ArrayList<>();
        children.add(42L);

        for (int i = 0; i < 2 * d; ++i) {
            keys.add(new IntDataBox(i));
            children.add((long) i);

            InnerNode inner = new InnerNode(metadata, bufferManager, keys, children, treeContext);
            long pageNum = inner.getPage().getPageNum();
            InnerNode parsed = InnerNode.fromBytes(metadata, bufferManager, treeContext, pageNum);
            assertEquals(inner, parsed);
        }
    }
}
