#ifndef __THREAD_POOL_H_
#define __THREAD_POOL_H_
#include <pthread.h>
/* 线程池结构体 */
typedef struct threadpool_t
{
    /* 线程池中的线程 */
    pthread_t *threadId;
    pthread_t *threadIds;

    /* 最小的线程数 */
    int minThreads;
    /* 最大的线程数 */
    int maxThreads;
}threadpool_t;

/* 线程池初始化 */
int threadPoolInit(threadpool_t *pool, int minThreads, int maxThreads);

/* 线程池销毁 */
int threadPoolDestroy(threadpool_t *pool);


#endif // __THREAD_POOL_H_