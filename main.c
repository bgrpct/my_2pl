#include <stdio.h>
#include <pthread.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>
#include <errno.h>

struct big_array {
    int item_cnt;
    int *items;
    pthread_rwlock_t **item_locks;
    pthread_rwlock_t item_locks_lock;
};

int big_array_init(struct big_array *arr, const int N)
{
    if (N <= 0) {
        printf("ERROR: invalid N value: %d\n", N);
        return EINVAL;
    }
    arr->item_cnt = N;

    // Create items array & item locks array
    arr->items = (int *)malloc(sizeof(int) * N);
    if (arr->items == NULL) {
        printf("ERROR: malloc() arr->items failed");
        return ENOMEM;
    }

    // Use some searching binary trees to keep rwlocks is better,
    // but in C, array is also a good choice
    arr->item_locks = (pthread_rwlock_t **)malloc(sizeof(pthread_rwlock_t *) * N);
    if (arr->item_locks == NULL) {
        printf("ERROR: malloc() arr->item_locks failed");
        return ENOMEM;
    }

    for (int i = 0; i < N; i++) {
        arr->items[i] = i;
        arr->item_locks[i] = NULL;
    }

    // Init a rwlock to protect our item locks array, cause we
    // only create a new rwlock for item which will be accessed
    int rc = pthread_rwlock_init(&arr->item_locks_lock, NULL);
    if (rc != 0) {
        printf("ERROR: pthread_rwlock_init() = %d\n", rc);
        return rc;
    }

    return 0;
}

// Lazy initializate rwlock for each array item
pthread_rwlock_t *big_array_get_item_lock(struct big_array *arr, int id)
{
    // This function could be optimized by C++ call_once(),
    // but in C, double-checked locking is a worked way.
    pthread_rwlock_t *target_lock = arr->item_locks[id];
    if (target_lock == NULL) {
        pthread_rwlock_wrlock(&arr->item_locks_lock);
        if (arr->item_locks[id] != NULL) {
            target_lock = arr->item_locks[id];
        } else {
            pthread_rwlock_t *new_lock = (pthread_rwlock_t *)malloc(sizeof(pthread_rwlock_t));
            if (new_lock == NULL) {
                pthread_rwlock_unlock(&arr->item_locks_lock);
                return NULL;
            }

            pthread_rwlock_init(new_lock, NULL);
            arr->item_locks[id] = new_lock;
            target_lock = new_lock;
        }
        pthread_rwlock_unlock(&arr->item_locks_lock);
    }

    return target_lock;
}

int big_array_read_item_without_lock(struct big_array *arr, int id)
{
    return arr->items[id];
}

int big_array_write_item_without_lock(struct big_array *arr, int id, int new_value)
{
    int old_value = arr->items[id];
    arr->items[id] = new_value;
    return old_value;
}

struct worker {
    int id;
    struct big_array *arr;
    int loop_cnt;
    struct timespec time_used;
    int current_i;
    int current_j;
    double ops;
    pthread_t th;
};

void worker_print_info(struct worker *w)
{
    printf("INFO: id = %d, loop_cnt = %d, time_used = %lds%ldns, ops = %.2f\n",
           w->id, w->loop_cnt, w->time_used.tv_sec, w->time_used.tv_nsec, w->ops);
}

void worker_release_locks(struct big_array *arr, int *write_item_locked, int *read_items_locked, int i, int j)
{
    pthread_rwlock_t *write_lock = NULL;
    pthread_rwlock_t *read_lock = NULL;
    int rc = 0;

    if (*write_item_locked == 1) {
        write_lock = big_array_get_item_lock(arr, j);
        assert(write_lock != NULL);

        rc = pthread_rwlock_unlock(write_lock);
        assert(rc == 0);

        *write_item_locked = 0;
    }

    for (int id = 0; id < 3; id++) {
        // Unlock rwlock too many times will make lock-leak
        if (j == (i + id) % arr->item_cnt) {
            read_items_locked[id] = 0;
            continue;
        }

        if (read_items_locked[id] == 1) {
            read_lock = big_array_get_item_lock(arr, (i + id) % arr->item_cnt);
            assert(read_lock != NULL);

            rc = pthread_rwlock_unlock(read_lock);
            assert(rc == 0);

            read_items_locked[id] = 0;
        }
    }
}

void worker_do_transaction(struct worker *w)
{
    struct big_array *arr = w->arr;
    int i0 = w->current_i;
    int i1 = (i0 + 1) % arr->item_cnt;
    int i2 = (i0 + 2) % arr->item_cnt;
    int j = w->current_j;
    int read_items_locked[3] = { 0, 0, 0 };
    int write_item_locked = 0;

    // Two phases lock
    // Start transaction: Expanding phase
    int begin_time = time(NULL);
    while (write_item_locked + read_items_locked[0] + read_items_locked[1] + read_items_locked[2] < 4) {
        // Timeout (> 2s)! Release all locks in hand to avoid dead lock then try to get all target locks again after sleeping
        int end_time = time(NULL);
        if (end_time - begin_time > 2) {
            // Sleep differnet time for each thread to avoid conflict
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            srand(ts.tv_nsec + w->id);
            int sleep_ms = rand() % 100;  // Sleep time < 100ms

            printf("TIMEOUT: thread%d sleep %dms, current [i, j]: [%d, %d], locked: [i, i+1, i+2, j]: [%d, %d, %d, %d]\n",
                   w->id, sleep_ms, i0, j, write_item_locked, read_items_locked[0], read_items_locked[1], read_items_locked[2]);

            worker_release_locks(arr, &write_item_locked, read_items_locked, i0, j);

            ts.tv_sec = 0;
            ts.tv_nsec = sleep_ms * 1000000;
            nanosleep(&ts, NULL);

            begin_time = time(NULL);
        }

        // Try lock target items, without blocking
        if (write_item_locked == 0) {
            pthread_rwlock_t *write_lock = big_array_get_item_lock(arr, j);
            int rc = pthread_rwlock_trywrlock(write_lock);
            if (rc == 0) {
                write_item_locked = 1;

                // Avoid trying to get read-lock for the same item
                if (j == i0)
                    read_items_locked[0] = 1;
                else if (j == i1)
                    read_items_locked[1] = 1;
                else if (j == i2)
                    read_items_locked[2] = 1;
            } else {
                // Try to get write target's lock firstly
                continue;
            }
        }

        for (int id = 0; id < 3; id++) {
            if (read_items_locked[id] == 0) {
                pthread_rwlock_t *rwlock = big_array_get_item_lock(arr, (i0 + id) % arr->item_cnt);
                assert(rwlock != NULL);

                int rc = pthread_rwlock_tryrdlock(rwlock);
                if (rc == 0)
                    read_items_locked[id] = 1;
            }
        }
    }

    // Update target itmes data
    int value_i0 = big_array_read_item_without_lock(arr, i0);
    int value_i1 = big_array_read_item_without_lock(arr, i1);
    int value_i2 = big_array_read_item_without_lock(arr, i2);
    big_array_write_item_without_lock(arr, j, value_i0 + value_i1 + value_i2);

    // End transaction: Shrinking phase
    worker_release_locks(arr, &write_item_locked, read_items_locked, i0, j);
}

void* worker_run(void *worker)
{
    struct worker *w = worker;
    int loop_id = 0;

    struct timespec begin_time;
    clock_gettime(CLOCK_REALTIME, &begin_time);

    for (loop_id = 0; loop_id < w->loop_cnt; loop_id++) {
        // Using time and thread id as random number seed
        // to generate exclusive i & j for each worker thread
        struct timespec current_time;
        clock_gettime(CLOCK_REALTIME, &current_time);
        srand(current_time.tv_nsec + w->id);
        w->current_i = rand() % w->arr->item_cnt;
        w->current_j = rand() % w->arr->item_cnt;

        worker_do_transaction(w);
    }

    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);

    // Check time this worker thread has spent
    long long unsigned ns_used = (end_time.tv_sec - begin_time.tv_sec) * 1000000000 + (end_time.tv_nsec - begin_time.tv_nsec);
    w->time_used.tv_sec = ns_used / 1000000000;
    w->time_used.tv_nsec = ns_used % 1000000000;

    // Calculate option per second
    w->ops = w->loop_cnt / (ns_used / 1000000000.0);

    printf("DONE: thread%d\n", w->id);
    return NULL;
}

// N: data item count
// M: reader/writer count
int worker_all_run(const int loop_cnt, const int N, const int M)
{
    struct big_array arr;
    int rc = 0;

    struct timespec begin_time;
    clock_gettime(CLOCK_REALTIME, &begin_time);

    rc = big_array_init(&arr, N);
    if (rc != 0) {
        printf("ERROR: big_array_init() failed: rc = %d\n", rc);
        return rc;
    }

    // Init workers & make them running
    struct worker workers[M];
    for (int i = 0; i < M; i++) {
        workers[i].id = i;
        workers[i].arr = &arr;
        workers[i].loop_cnt = loop_cnt;
        workers[i].time_used.tv_sec = 0;
        workers[i].time_used.tv_nsec = 0;
        workers[i].current_i = 0;
        workers[i].current_j = 0;
        workers[i].ops = 0.0;

        pthread_create(&workers[i].th, NULL, worker_run, &workers[i]);
    }

    // Wait all worker threads
    for (int i = 0; i < M; i++) {
        pthread_join(workers[i].th, NULL);
    }

    // Show info of all worker
    struct timespec end_time;
    clock_gettime(CLOCK_REALTIME, &end_time);

    double avg_ops = 0.0;
    for (int i = 0; i < M; i++) {
        worker_print_info(&workers[i]);
        avg_ops += (workers[i].ops / M);
    }
    long long unsigned ns_used = (end_time.tv_sec - begin_time.tv_sec) * 1000000000 + (end_time.tv_nsec - begin_time.tv_nsec);
    struct timespec time_used = { .tv_sec = ns_used / 1000000000, .tv_nsec = ns_used % 1000000000 };
    printf("\nSUMMARY: time_used: %lds%ldns, avg_ops: %.2f\n", time_used.tv_sec, time_used.tv_nsec, avg_ops);

    return 0;
}

int main()
{
    int N = 100000;
    int M = 64;
    int loop_cnt = 10000;
    return worker_all_run(loop_cnt, N, M);
}