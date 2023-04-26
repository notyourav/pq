#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include "pq.h"

#define PARENT(i) ((i - 1) / 2)
#define LEFT(i) (2 * i + 1)
#define RIGHT(i) (2 * i + 2)
#define XCHG(a, b) { pq_node_t temp = a; a = b; b = temp; }
#define PQSIZE 1024
#define BLOCKSIZE BUFSIZ
#define CACHESIZE (BLOCKSIZE / sizeof(int))

static void pq_heapify(pq_t pq, int i);
static void pq_insert(pq_t pq, int idx, int item);

static int pq_block_find(pq_t pq, int idx, int item);
static void pq_block_split(pq_t pq, int idx);
static void pq_block_exchange(pq_t pq, int idx1, int idx2);

static void cache_set(pq_t pq, int idx);
static void cache_insert(pq_t pq, int size, int idx);
static void cache_flush(pq_t pq);
static void cache_flush_idx(pq_t pq, int idx);
static void cache_invalidate(pq_t pq);

typedef struct pq_node {
    int min, max;
    int size;
} pq_node_t;

typedef struct pq {
    pq_node_t nodes[PQSIZE];
    int cache[CACHESIZE]; // cache for the block
    int cached_idx; // index of the cached block
    int size;
    int fd;
} *pq_t;

// create a new priority queue
// initial block has restriction on range
pq_t pq_open(const char* filename) {
    struct pq* pq = malloc(sizeof(struct pq));

    pq->nodes[0].min = INT32_MIN;
    pq->nodes[0].max = INT32_MAX;
    pq->nodes[0].size = 0;
    pq->size = 1;
    cache_set(pq, 0);

    pq->fd = open(filename, O_RDWR | O_CREAT, S_IRWXU);
    if (pq->fd != -1) {
        return pq;
    }

    free(pq);
    perror("pq_open");
    return NULL;
}

void pq_enqueue(pq_t pq, int item) {
    int idx = pq_block_find(pq, 0, item);

    if (idx != -1) {
        pq_insert(pq, idx, item);
    } else {
        fprintf(stderr, "pq_enqueue: could not find block to insert item\n");
        fprintf(stderr, "pq_enqueue: item = %d\n", item);
        exit(1);
    }
}

// dequeue the highest priority item
// maxheap structure gaurantees that the highest priority item is at the root
// if root is empty, then rebalance the tree
int pq_dequeue(pq_t pq) {
    if (pq->size != 0) {
        pq_node_t* node = &pq->nodes[0];

        cache_set(pq, 0);
        int item = pq->cache[node->size - 1];

        node->size--;
        if (node->size == 0) {
            cache_flush(pq);
            cache_invalidate(pq);
            pq->nodes[0] = pq->nodes[pq->size - 1];
            pq_block_exchange(pq, 0, pq->size - 1);
            pq->size--;
            pq_heapify(pq, 0);
        }

        return item;
    }

    fprintf(stderr, "pq_dequeue: heap underflow\n");
    return -1;
}

void pq_close(pq_t pq) {
    cache_flush(pq);
    close(pq->fd);
    free(pq);
}

static void pq_heapify(pq_t pq, int i) {
    int l = LEFT(i);
    int r = RIGHT(i);

    int largest = i;
    if (l < pq->size && pq->nodes[l].max > pq->nodes[i].max) {
        largest = l;
    }

    if (r < pq->size && pq->nodes[r].max > pq->nodes[largest].max) {
        largest = r;
    }

    if (largest != i) {
        pq_block_exchange(pq, i, largest);
        XCHG(pq->nodes[i], pq->nodes[largest]);
        pq_heapify(pq, largest);
    }
    cache_set(pq, i);
}

static void pq_block_exchange(pq_t pq, int idx1, int idx2) {
    int* temp = malloc(BLOCKSIZE);

    // idx1 -> temp
    cache_set(pq, idx1);
    memcpy(temp, pq->cache, pq->nodes[idx1].size * sizeof(int));

    // idx2 -> idx1
    cache_set(pq, idx2);
    cache_flush_idx(pq, idx1);

    // temp -> idx2
    memcpy(pq->cache, temp, pq->nodes[idx1].size * sizeof(int));

    free(temp);
}

// recursively find the block that can contain the item
static int pq_block_find(pq_t pq, int idx, int item) {
    if (idx < pq->size) {
        if (item >= pq->nodes[idx].min && item <= pq->nodes[idx].max) {
            return idx;
        } else {
            int l = pq_block_find(pq, LEFT(idx), item);
            int r = pq_block_find(pq, RIGHT(idx), item);

            if (l != -1) {
                return l;
            } else if (r != -1) {
                return r;
            }
        }
    }

    return -1;
}

// insert a value into a block
static void pq_insert(pq_t pq, int idx, int item) {
    pq_node_t* node = &pq->nodes[idx];

    cache_set(pq, idx);
    cache_insert(pq, node->size, item);
    node->size++;
    if (node->size >= CACHESIZE) {
        pq_block_split(pq, idx);
        pq_heapify(pq, 0);
    }
}

static void pq_block_split(pq_t pq, int idx) {
    pq_node_t* node = &pq->nodes[idx];
    pq_node_t* new_node = &pq->nodes[pq->size];

    // printf("pq_block_split: splitting block %d: [%d, %d], size %d\n", idx, node->min, node->max, node->size);

    cache_flush(pq);


    // "median"
    int mid = pq->cache[node->size / 2];

    // upper half of the block
    new_node->min = mid + 1;
    new_node->max = node->max;
    new_node->size = node->size - node->size / 2;

    // lower half of the block
    node->min = node->min;
    node->max = mid;
    node->size = node->size / 2;

    // overwrite the lower half of the block with the upper half
    pq->cached_idx = pq->size;
    for (int i = 0; i < new_node->size; i++) {
        pq->cache[i] = pq->cache[i + node->size];
    }

    pq->size++;
}

// set the cache to the block at idx
static void cache_set(pq_t pq, int idx) {
    if (pq->cached_idx != idx) {
        cache_flush(pq);

        lseek(pq->fd, idx * BLOCKSIZE, SEEK_SET);
        read(pq->fd, pq->cache, BLOCKSIZE);
        pq->cached_idx = idx;
    }
}

// insertion sort
static void cache_insert(pq_t pq, int size, int item) {
    int i = size - 1;
    while (i >= 0 && pq->cache[i] > item) {
        assert(pq->cache[i] <= pq->nodes[pq->cached_idx].max);
        assert(pq->cache[i] >= pq->nodes[pq->cached_idx].min);
        pq->cache[i + 1] = pq->cache[i];
        i--;
    }

    pq->cache[i + 1] = item;
}

static void cache_flush(pq_t pq) {
    cache_flush_idx(pq, pq->cached_idx);
}

static void cache_flush_idx(pq_t pq, int idx) {
    if (idx != -1) {
        lseek(pq->fd, idx * BLOCKSIZE, SEEK_SET);
        write(pq->fd, pq->cache, pq->nodes[idx].size * sizeof(int));
    }
}

void print_block_content(pq_t pq) {
    int buf[CACHESIZE];

    cache_flush(pq);

    for (int i = 0; i < pq->size; i++) {
        lseek(pq->fd, i * BLOCKSIZE, SEEK_SET);
        read(pq->fd, buf, BLOCKSIZE);

        printf("block %d: sz=%d, min=%d, max=%d\n", i, pq->nodes[i].size, pq->nodes[i].min, pq->nodes[i].max);
        for (int j = 0; j < pq->nodes[i].size; j++) {
            printf("%d ", buf[j]);
        }
        printf("\n");
    }
}

static void cache_invalidate(pq_t pq) {
    pq->cached_idx = -1;
}
