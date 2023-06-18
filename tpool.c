#include <pthread.h>
#include <stdlib.h>
#include "tpool.h"


struct tpool_work {
    thread_func_t      func;        // function to call.
    void              *arg;         // function arguments.
    struct tpool_work *next;        // next element.
};
typedef struct tpool_work tpool_work_t;


struct tpool {
    tpool_work_t    *work_first;    // pointer to the first item in the list.
    tpool_work_t    *work_last;     // pointer to the last item in the list.
    pthread_mutex_t  work_mutex;    // mutex for locking threads.
    pthread_cond_t   work_cond;     // signals the threads that there is work to be processed.
    pthread_cond_t   working_cond;  // signals when there are no threads processing.
    size_t           working_cnt;   // to know how many threads are actively processing work.
    size_t           thread_cnt;    // number of active threads.
    bool             stop;          // stops the threads.
};


/**
 * \brief Allocates memory for work.
 * 
 * \return Pointer to work.
 */
static tpool_work_t *tpool_work_create(thread_func_t func, void *arg)
{
    tpool_work_t *work;

    if (func == NULL)
        return NULL;

    work       = malloc(sizeof(*work));
    work->func = func;
    work->arg  = arg;
    work->next = NULL;
    return work;
}


/**
 * \brief The function frees up memory after the work has been done.
 */
static void tpool_work_destroy(tpool_work_t *work)
{
    if (work == NULL)
        return;
    free(work);
}


/**
 * \brief The function returns the work for the thread from the list.
 * 
 * \context Work will need to be pulled from the queue at some point to be processed.
 * Since the queue is a linked list this handles not only pulling an object from the list
 * but also maintaining the list work_first and work_last references for us.
 * 
 * \return Pointer to the work list.
 */
static tpool_work_t *tpool_work_get(tpool_t *tm)
{
    tpool_work_t *work;

    if (tm == NULL)
        return NULL;

    work = tm->work_first;
    if (work == NULL)
        return NULL;

    if (work->next == NULL) {
        tm->work_first = NULL;
        tm->work_last  = NULL;
    } else {
        tm->work_first = work->next;
    }

    return work;
}


/**
 * \brief The function waits and processes the work.
 * 
 * \context The function is waiting for work. As soon as the signal is received,
 * the function gets the work and starts processing it.
 * After completing the work, it must be removed from the list.
 */
static void *tpool_worker(void *arg)
{
    tpool_t      *tm = arg;
    tpool_work_t *work;

    while (1) {
        pthread_mutex_lock(&(tm->work_mutex));

        while (tm->work_first == NULL && !tm->stop)
            pthread_cond_wait(&(tm->work_cond), &(tm->work_mutex));

        if (tm->stop)
            break;

        work = tpool_work_get(tm);

        pthread_mutex_unlock(&(tm->work_mutex));

        if (work != NULL) {
            work->func(work->arg);
            tpool_work_destroy(work);
        }

        pthread_mutex_lock(&(tm->work_mutex));
        tm->working_cnt--;
        if (!tm->stop && tm->working_cnt == 0 && tm->work_first == NULL)
            pthread_cond_signal(&(tm->working_cond));
        pthread_mutex_unlock(&(tm->work_mutex));
    }

    tm->thread_cnt--;
    pthread_cond_signal(&(tm->working_cond));
    pthread_mutex_unlock(&(tm->work_mutex));
    return NULL;
}


/**
 * \brief Threadpool create.
 * 
 * \context When creating the pool the default is two threads if zero was specified.
 * Otherwise, the caller specified number will be used.
 * The requested number of threads are started and tpool_worker is specified as the thread function.
 * The threads are detached so they will cleanup on exit.
 * 
 * \return Pointer to threadpool.
 */
tpool_t *tpool_create(size_t num)
{
    tpool_t   *tm;
    pthread_t  thread;
    size_t     i;

    if (num == 0)
        num = 2;

    tm             = calloc(1, sizeof(*tm));
    tm->thread_cnt = num;

    pthread_mutex_init(&(tm->work_mutex), NULL);
    pthread_cond_init(&(tm->work_cond), NULL);
    pthread_cond_init(&(tm->working_cond), NULL);

    tm->work_first = NULL;
    tm->work_last  = NULL;

    for (i=0; i<num; i++) {
        pthread_create(&thread, NULL, tpool_worker, tm);
        pthread_detach(thread);
    }

    return tm;
}


/**
 * \brief Threadpool destroy.
 * 
 * \context Typically, destroy will only be called when all work is done and nothing is processing.
 * However, it’s possible someone is trying to force processing to stop.
 * In which case there will be work queued and we need to get clean it up.
 */
void tpool_destroy(tpool_t *tm)
{
    tpool_work_t *work;
    tpool_work_t *work2;

    if (tm == NULL)
        return;

    pthread_mutex_lock(&(tm->work_mutex));
    work = tm->work_first;
    while (work != NULL) {
        work2 = work->next;
        tpool_work_destroy(work);
        work = work2;
    }
    tm->stop = true;
    pthread_cond_broadcast(&(tm->work_cond));
    pthread_mutex_unlock(&(tm->work_mutex));

    tpool_wait(tm);

    pthread_mutex_destroy(&(tm->work_mutex));
    pthread_cond_destroy(&(tm->work_cond));
    pthread_cond_destroy(&(tm->working_cond));

    free(tm);
}


/**
 * \brief Adding work to the queue.
 * 
 * \context Adding to the work queue consists of creating a work object,
 * locking the mutex and adding the object to the liked list.
 * 
 * \return If the work is added, it returns true, otherwise false.
 */
bool tpool_add_work(tpool_t *tm, thread_func_t func, void *arg)
{
    tpool_work_t *work;

    if (tm == NULL)
        return false;

    work = tpool_work_create(func, arg);
    if (work == NULL)
        return false;

    pthread_mutex_lock(&(tm->work_mutex));
    if (tm->work_first == NULL) {
        tm->work_first = work;
        tm->work_last  = tm->work_first;
    } else {
        tm->work_last->next = work;
        tm->work_last       = work;
    }
    tm->working_cnt++;
    pthread_cond_broadcast(&(tm->work_cond));
    pthread_mutex_unlock(&(tm->work_mutex));

    return true;
}


/**
 * \brief Waiting for processing to complete.
 * 
 * \context This is a blocking function that will only return when there is no work.
 * The mutex is locked and we wait in a conditional if there are any threads processing,
 * if there is still work to do or if the threads are stopping and not all have exited.
 * The retry is a safety measure in case of spurious wake ups.
 * Once there is nothing processing return so the caller can continue.
 */
void tpool_wait(tpool_t *tm)
{
    if (tm == NULL)
        return;

    pthread_mutex_lock(&(tm->work_mutex));
    while (1) {
        if ((!tm->stop && tm->working_cnt != 0) || (tm->stop && tm->thread_cnt != 0)) {
            pthread_cond_wait(&(tm->working_cond), &(tm->work_mutex));
        } else {
            break;
        }
    }
    pthread_mutex_unlock(&(tm->work_mutex));
}
