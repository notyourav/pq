#include <stdio.h>
#include <stdlib.h>
#include "pq.h"

int main() {
    pq_t pq = pq_open("example.txt");

    for (int i = 0; i < 1024; i++) {
        // int r = rand() % INT32_MAX;
        pq_enqueue(pq, i);
    }

    FILE* f = fopen("out.txt", "w");

    // print_block_content(pq);

    for (int i = 0; i < 1024; i++) {
        int v = pq_dequeue(pq);
        fprintf(f, "%d\n", v);
    }
    pq_close(pq);
    fclose(f);
}
