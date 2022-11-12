package edu.berkeley.cs186.database.concurrency;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    static Boolean[][] COMPATIBLE_MATRIX = new Boolean[][]{
            {true, true, true, true, true, true},
            {true, true, true, true, true, false},
            {true, true, true, false, false, false},
            {true, true, false, true, false, false},
            {true, true, false, false, false, false},
            {true, false, false, false, false, false}
    };
    static Boolean[][] PARENT_MATRIX = new Boolean[][]{
            {true, false, false, false, false, false},
            {true, true, false, true, false, false},
            {true, true, true, true, true, true},
            {true, true, false, true, false, false},
            {true, true, true, true, true, true},
            {true, true, true, true, true, true}
    };

    static Boolean[][] SUBSTITUTABILITY_MATRIX = new Boolean[][]{
            {true, false, false, false, false, false},
            {true, true, false, false, false, false},
            {true, true, true, false, false, false},
            {true, true, false, true, false, false},
            {true, true, true, true, true, false},
            {true, true, true, true, true, true}
    };

    static LockType[] LOCK_TYPE_ARRAY = new LockType[]{NL, IS, IX, S, SIX, X};
    static Map<LockType, Integer> LOCK_TYPE_TO_INDEX_MAP = new HashMap<>();

    static {
        for (int i = 0; i < LOCK_TYPE_ARRAY.length; i++) {
            LOCK_TYPE_TO_INDEX_MAP.put(LOCK_TYPE_ARRAY[i], i);
        }
    }

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement

        Integer indexA = LOCK_TYPE_TO_INDEX_MAP.get(a);
        Integer indexB = LOCK_TYPE_TO_INDEX_MAP.get(b);
        if (indexA == null || indexB == null) {
            throw new IllegalArgumentException("not support lock type: " + a + " or " + b);
        }
        return COMPATIBLE_MATRIX[indexA][indexB];
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
            case S:
                return IS;
            case X:
                return IX;
            case IS:
                return IS;
            case IX:
                return IX;
            case SIX:
                return IX;
            case NL:
                return NL;
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement
        Integer indexParent = LOCK_TYPE_TO_INDEX_MAP.get(parentLockType);
        Integer indexChild = LOCK_TYPE_TO_INDEX_MAP.get(childLockType);
        if (indexParent == null || indexChild == null) {
            throw new IllegalArgumentException("not support lock type: " + parentLockType + " or " + childLockType);
        }
        return PARENT_MATRIX[indexParent][indexChild];
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // DONE(proj4_part1): implement
        Integer indexSubstitute = LOCK_TYPE_TO_INDEX_MAP.get(substitute);
        Integer indexRequired = LOCK_TYPE_TO_INDEX_MAP.get(required);
        if (indexSubstitute == null || indexRequired == null) {
            throw new IllegalArgumentException("not support lock type: " + substitute + " or " + required);
        }
        return SUBSTITUTABILITY_MATRIX[indexSubstitute][indexRequired];
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
            case S:
                return "S";
            case X:
                return "X";
            case IS:
                return "IS";
            case IX:
                return "IX";
            case SIX:
                return "SIX";
            case NL:
                return "NL";
            default:
                throw new UnsupportedOperationException("bad lock type");
        }
    }
}

