#pragma once

typedef struct pq* pq_t;

pq_t pq_open(const char* filename);
void pq_enqueue(pq_t pq, int item);
// todo: dequeue does not propogate the min/max values when it merges
int pq_dequeue(pq_t pq);
void pq_close(pq_t pq);

void print_block_content(pq_t pq);
