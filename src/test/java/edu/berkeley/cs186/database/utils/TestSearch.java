package edu.berkeley.cs186.database.utils;

import edu.berkeley.cs186.database.categories.Proj2Tests;
import edu.berkeley.cs186.database.categories.PublicTests;
import edu.berkeley.cs186.database.databox.DataBox;
import edu.berkeley.cs186.database.databox.IntDataBox;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Category(Proj2Tests.class)
public class TestSearch {
    @Test
    @Category(PublicTests.class)
    public void testBinarySearch() {

        List<TestCase> testCases = new ArrayList<TestCase>() {{
            add(new TestCase<>(null, 2, -1));
            add(new TestCase<>(Arrays.asList(), 2, -1));
            add(new TestCase<>(Arrays.asList(1), 2, -1));
            add(new TestCase<>(Arrays.asList(1), 1, 0));
            add(new TestCase<>(Arrays.asList(1, 2), 3, -1));
            add(new TestCase<>(Arrays.asList(1, 2), 1, 0));
            add(new TestCase<>(Arrays.asList(1, 2), 2, 1));
            add(new TestCase<>(Arrays.asList(1, 2, 3, 4, 5, 6), 2, 1));
            add(new TestCase<>(Arrays.asList(1, 2, 3, 4, 5, 6), 1, 0));
            add(new TestCase<>(Arrays.asList(1, 2, 3, 4, 5, 6), 6, 5));
            add(new TestCase<>(Arrays.asList(1, 2, 2, 2, 51, 69, 100), 2, 1));
            add(new TestCase<>(Arrays.asList(1, 2, 2, 2, 51, 69, 100), 53, -1));
            add(new TestCase<>(Arrays.asList(1, 2, 2, 2, 51, 69, 100), 51, 4));
            add(new TestCase<>(Arrays.asList(new IntDataBox(1), new IntDataBox(1),
                    new IntDataBox(1), new IntDataBox(3), new IntDataBox(11), new IntDataBox(111)), new IntDataBox(111), 5));
            add(new TestCase<>(Arrays.asList(new IntDataBox(1), new IntDataBox(1),
                    new IntDataBox(1), new IntDataBox(3), new IntDataBox(11), new IntDataBox(111)), new IntDataBox(1), 0));
            add(new TestCase<>(Arrays.asList(new IntDataBox(1), new IntDataBox(1),
                    new IntDataBox(1), new IntDataBox(3), new IntDataBox(11), new IntDataBox(111)), new IntDataBox(11), 4));
        }};
        for (TestCase testCase : testCases) {
            int i = Search.binarySearch(testCase.getDatas(), testCase.getSearchData());
            assert i == testCase.getExcept();
        }
    }

    private class TestCase<T extends Comparable> {
        List<T> datas;
        T searchData;
        int except;

        public TestCase(List<T> datas, T searchData, int except) {
            this.datas = datas;
            this.searchData = searchData;
            this.except = except;
        }

        public int getExcept() {
            return except;
        }

        public List<T> getDatas() {
            return datas;
        }

        public T getSearchData() {
            return searchData;
        }
    }
}




