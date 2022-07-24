package edu.berkeley.cs186.database.utils;

import java.util.List;

public class Search {

    /**
     * binary search the first occurrence data that equals to {@param searchData} from a incremental list
     *
     * @param datas      incremental list
     * @param searchData the data to search
     * @param <T>        class extends {@link Comparable}
     * @return the index of the first occurrence of the specified element in this list, or -1 if this list does not contain the element
     */
    public static <T extends Comparable> int binarySearch(List<T> datas, T searchData) {
        if (datas == null || datas.size() <= 0) {
            return -1;
        }
        int candidate = -1;
        int left = 0;
        int right = datas.size() - 1;
        while (true) {
            if (right - left <= 1) {
                if (datas.get(right).compareTo(searchData) == 0) {
                    candidate = right;
                }
                if (datas.get(left).compareTo(searchData) == 0) {
                    candidate = left;
                }
                break;
            }
            int middle = (right + left) / 2;
            int compare = datas.get(middle).compareTo(searchData);
            if (compare == 0) {
                candidate = middle;
                break;
            } else if (compare < 0) {
                left = middle;
            } else {
                right = middle;
            }
        }
        if (candidate < 0) {
            return -1;
        }
        //find first occurrence data
        while (candidate > 0) {
            candidate--;
            if (datas.get(candidate).compareTo(searchData) != 0) {
                candidate++;
                break;
            }
        }
        return candidate;
    }
}
