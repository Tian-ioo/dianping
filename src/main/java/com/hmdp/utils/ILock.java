package com.hmdp.utils;

public interface ILock {
    /**
     * 尝试获取锁
     * @Param timeoutSec 锁的持有时间
     * @return true:获取成功 false:获取失败
     */
    boolean tryLock(long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();

}
