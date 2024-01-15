#include "threadPool.h"
#include <stdio.h>
#include <stdlib.h>
#include <error.h>
#include <string.h>

#define DEFAULT_MIN_THREADS     5
#define DEFAULT_MAX_THREADS     10
#define DEFAULT_QUEUE_CAPACITY  100

#define TIME_INTERVAL           5

/* 扩容/缩容每次增加或减少的默认线程池 */
#define DEFAULT_VARY_THREADS    3

enum STATUS_CODE
{
    ON_SUCCESS,
    NULL_PTR,
    MALLOC_ERROR,
    ACCESS_INVAILD,
    UNKNOWN_ERROR,
};

/* 静态函数前置声明 */
static void * threadHander(void *arg);
static void * managerHander(void *arg);
static int threadExitClrResources(threadpool_t *pool);


/* 线程退出清理资源 */
static int threadExitClrResources(threadpool_t *pool)
{
    for (int idx = 0; idx < pool->maxThreads; idx++)
    {
        if (pool->threadIds[idx] == pthread_self())
        {
            pool->threadIds[idx] = 0;
            break;
        }
    }
    pthread_exit(NULL);
}

/* 本质是一个消费者 */
static void * threadHander(void *arg)
{
    /* 强制类型转换. */
    threadpool_t *pool = (threadpool_t *)arg;
    while (1)
    {
        pthread_mutex_lock(&(pool->mutexpool));
        while (pool->queueSize == 0 && pool->shutDown != 0)
        {
            /* 等待一个条件变量. 生产者发送过来的. */
            pthread_cond_wait(&(pool->notEmpty), &(pool->mutexpool));

            if (pool->exitThreadNums > 0)
            {
                /* 离开数减一 */
                pool->exitThreadNums--;
                if (pool->liveThreadNums > pool->minThreads)
                {
                    /* 线程退出 */
                    threadExitClrResources(pool);
                }
            }
        }

        if (pool->shutDown)
        {
            /* 线程退出 */
            threadExitClrResources(pool);
        }


        /* 意味着任务队列有任务 */
        task_t tmpTask = pool->taskQueue[pool->queueFront];
        pool->queueFront = (pool->queueFront + 1) % pool->queueCapacity;
        /* 任务数减一 */
        pool->queueSize--;

        pthread_mutex_unlock(&(pool->mutexpool));
        /* 发一个信号给生产者 告诉他可以继续生产. */
        pthread_cond_signal(&pool->notFull);


        /* 为了提升我们性能, 再创建一把只维护busyNum属性的锁. */
        pthread_mutex_lock(&pool->mutexBusy);
        pool->busyThreadNums++;
        pthread_mutex_unlock(&pool->mutexBusy);

        /* 执行钩子函数 - 回调函数. */
        tmpTask.worker_hander(tmpTask.arg);
        /* 释放堆空间 todo... */

        pthread_mutex_lock(&pool->mutexBusy);
        pool->busyThreadNums--;
        pthread_mutex_unlock(&pool->mutexBusy);
    }
    pthread_exit(NULL);
}

/* 管理者线程 */
static void * managerHander(void *arg)
{
    /* 强制类型转换. */
    threadpool_t *pool = (threadpool_t *)arg;

    while (pool->shutDown == 0)
    {
        sleep(TIME_INTERVAL);

        pthread_mutex_lock(&pool->mutexpool);
        /* 任务队列任务数 */
        int taskNums = pool->queueSize;
        /* 存活的线程数 */
        int liveThreadNums = pool->liveThreadNums;
        pthread_mutex_unlock(&pool->mutexpool);

        pthread_mutex_lock(&pool->mutexBusy);
        /* 忙的线程数 */ 
        int busyThreadNums = pool->busyThreadNums;
        pthread_mutex_unlock(&pool->mutexBusy);
        


        /* 扩容: 扩大线程池里面的线程数 (上限不要超过maxThreads) */
        /* 任务队列任务数 > 存活的线程数 && 存活的线程数 < maxThreads */
        if (taskNums > liveThreadNums && liveThreadNums < pool->maxThreads)
        {
            pthread_mutex_lock(&(pool->mutexpool));
            /* 计数 */
            int count = 0;
            /* 一次扩3个线程 */
            int ret = 0;
            for (int idx = 0; idx < pool->maxThreads && count < DEFAULT_VARY_THREADS &&liveThreadNums <= pool->maxThreads; idx++)
            {
                /* 能够用的索引位置 */
                if (pool->threadIds[idx] == 0)
                {
                    ret = pthread_create(&(pool->threadIds[idx]), NULL, threadHander, pool);
                    if (ret != 0)
                    {
                        perror("thread create error");
                        /* todo... */
                    }
                    /* 计数加一 */
                    count++;
                    /* 存活的线程数加一 */
                    pool->liveThreadNums++;
                }
            }
            pthread_mutex_unlock(&(pool->mutexpool));
        }

        /* 缩容: 减少线程池里面的线程数 (下限不要低于minThreads) */
        /* 忙的线程数 * 2 < 存活的线程数 && 存活的线程数 > minThreads */
        if ((busyThreadNums >> 1) < liveThreadNums && liveThreadNums > pool->minThreads)
        {
            pthread_mutex_lock(&(pool->mutexpool));
            /* todo... */
            /* 让摸鱼不干活的自己走 */
            #if 0
            for (int idx = 0; idx < pool->maxThreads; idx++)
            {
                if (pool->threadIds[idx] != 0)
                {
                    /* 误删 --- 这边肯定不对. 分不清楚谁在摸鱼，谁在干活. */
                    pthread_cancel(pool->threadIds[idx]);
                }
            }
            #endif

            /* 离开的线程数 */
            pool->exitThreadNums = DEFAULT_VARY_THREADS;
            for (int idx = 0; idx < DEFAULT_VARY_THREADS; idx++)
            {
                /* 发一个信号 */
                pthread_cond_signal(&(pool->notEmpty));
            }
            pthread_mutex_unlock(&(pool->mutexpool));
        }

    }

    /* 线程关闭 */
    pthread_exit(NULL);
}

/* 线程池初始化 */
int threadPoolInit(threadpool_t *pool, int minThreads, int maxThreads, int queueCapacity)
{
    if (pool == NULL)
    {
        return NULL_PTR;
    }
    
    do 
    {
        /* 判断合法性 */
        if (minThreads <= 0 || maxThreads <= 0 || minThreads >= maxThreads)
        {
            minThreads = DEFAULT_MIN_THREADS;
            maxThreads = DEFAULT_MAX_THREADS;
        }
        /* 更新线程池 线程属性 */
        pool->minThreads = minThreads;
        pool->maxThreads = maxThreads;
        /* 初始化时, 忙碌的线程数为0. */
        pool->busyThreadNums = 0;

        /* 判断合法性 */
        if (queueCapacity <= 0)
        {
            queueCapacity = DEFAULT_QUEUE_CAPACITY;
        }

        /* 更新线程池 任务队列属性 */
        pool->queueCapacity = queueCapacity;
        pool->taskQueue = (task_t *)malloc(sizeof(task_t) * pool->queueCapacity);
        if (pool->taskQueue == NULL)
        {
            perror("malloc error");
            break;
        }
        /* 清除脏数据 */
        memset(pool->taskQueue, 0, sizeof(task_t) * pool->queueCapacity);
        pool->queueFront = 0;
        pool->queueRear = 0;
        pool->queueSize = 0;

        /* 为线程ID分配堆空间 */
        pool->threadIds = (pthread_t *)malloc(sizeof(pthread_t) * pool->maxThreads);
        if (pool->threadIds == NULL)
        {
            perror("malloc error");
            break;
        }
        /* 清除脏数据 */
        memset(pool->threadIds, 0, sizeof(pthread_t) * pool->maxThreads);

        
        int ret = 0;
        ret = pthread_create(&(pool->managerThread), NULL, managerHander, pool);
        if (ret != 0)
        {
            perror("thread create error");
            break;
        }

        /* 创建线程 */
        for (int idx = 0; idx < pool->minThreads; idx++)
        {
            /* 如果线程ID号为0. 那个这个位置可以用. */
            if (pool->threadIds[idx] == 0)
            {
                ret = pthread_create(&(pool->threadIds[idx]), NULL, threadHander, pool);
                if (ret != 0)
                {
                    perror("thread create error");
                    break;
                }
            }
        }
        /* 此ret是创建线程函数的返回值. */
        if (ret != 0)
        {
            break;
        }
        /* 存活的线程数 等于 开辟线程数 */
        pool->liveThreadNums = pool->minThreads;

        /* 初始化锁资源 */
        pthread_mutex_init(&(pool->mutexpool), NULL);
        pthread_mutex_init(&(pool->mutexBusy), NULL);

        /* 初始化条件变量资源 */
        if (pthread_cond_init(&(pool->notEmpty), NULL) != 0 || pthread_cond_init(&(pool->notFull), NULL) != 0)
        {
            perror("thread cond error");
            break;
        }

        return ON_SUCCESS;
    } while(0);
    /* 程序执行到这个地方, 上面一定有失败. */

    /* 回收堆空间 */
    if (pool->taskQueue != NULL)
    {
        free(pool->taskQueue);
        pool->taskQueue = NULL;
    }
    
    /* 回收管理着线程资源 */
    pthread_join(pool->managerThread, NULL);

    /* 回收线程资源 */
    for (int idx = 0; idx < pool->minThreads; idx++)
    {
        if (pool->threadIds[idx] != 0)
        {
            pthread_join(pool->threadIds[idx], NULL);
        }
    }

    if (pool->threadIds != NULL)
    {
        free(pool->threadIds);
        pool->threadIds = NULL;
    }


    /* 释放锁资源 */
    pthread_mutex_destroy(&(pool->mutexpool));
    pthread_mutex_destroy(&(pool->mutexBusy));

    /* 释放 条件变量的资源 */
    pthread_cond_destroy(&(pool->notEmpty));
    pthread_cond_destroy(&(pool->notFull));

    return UNKNOWN_ERROR;
}



/* 线程池添加任务 */
int threadPoolAddTask(threadpool_t *pool, void *(worker_hander)(void *), void *arg)
{
    if (pool == NULL)
    {
        return NULL_PTR;
    }

    /* 加锁 */
    pthread_mutex_lock(&(pool->mutexpool));
    /* 任务队列满了 */
    while (pool->queueSize == pool->queueCapacity)
    {
        pthread_cond_wait(&(pool->notFull), &(pool->mutexpool));
    }
    /* 程序到这个地方一定有位置可以放任务 */
    /* 将任务放到队列的队尾 */
    pool->taskQueue[pool->queueRear].worker_hander = worker_hander;
    pool->taskQueue[pool->queueRear].arg = arg;
    /* 队尾向后移动. */
    pool->queueRear = (pool->queueRear + 1) % pool->queueCapacity;
    /* 任务数加一. */
    pool->queueSize++;

    pthread_mutex_unlock(&(pool->mutexpool));
    /* 发信号 */
    pthread_cond_signal(&(pool->notEmpty));

    return ON_SUCCESS;
}


/* 线程池销毁 */
int threadPoolDestroy(threadpool_t *pool)
{
    int ret;
    /* 标志位 */
    pool->shutDown = 1;

    /* 广播 */
    pthread_cond_broadcast(&(pool->notEmpty));

    
    return ret;
}