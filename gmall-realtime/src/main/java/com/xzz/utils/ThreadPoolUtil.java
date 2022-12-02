package com.xzz.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author 徐正洲
 * @create 2022-12-01 10:54
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor threadPoolExecutor;

    private ThreadPoolUtil() {

    }

    public static ThreadPoolExecutor getThreadPoolExecutor() {

        if (threadPoolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (threadPoolExecutor == null) {
                    threadPoolExecutor = new ThreadPoolExecutor(
                            4,
                            20,
                            100,
                            TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>());
                }
            }
        }

        return threadPoolExecutor;
    }

}
