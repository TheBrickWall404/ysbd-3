#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include "sort.h"
#include "merge.h"
#include "chunk.h"

bool shouldSwap(Record* rec1, Record* rec2) {
    int nameCmp = strcmp(rec1->name, rec2->name);
    if (nameCmp > 0) return true;
    if (nameCmp == 0 && strcmp(rec1->surname, rec2->surname) > 0) return true;
    return false;
}

// Helper for qsort
int compareRecords(const void* a, const void* b) {
    Record* r1 = (Record*)a;
    Record* r2 = (Record*)b;
    if (shouldSwap(r1, r2)) return 1; // r1 > r2
    if (shouldSwap(r2, r1)) return -1; // r2 > r1
    return 0;
}

void sort_Chunk(CHUNK* chunk) {
    if (chunk->recordsInChunk == 0) return;

    // 1. Load all records into memory
    Record* buffer = malloc(chunk->recordsInChunk * sizeof(Record));
    if (!buffer) {
        fprintf(stderr, "Memory allocation failed for sort_Chunk\n");
        exit(1);
    }

    for (int i = 0; i < chunk->recordsInChunk; i++) {
        CHUNK_GetIthRecordInChunk(chunk, i, &buffer[i]);
    }

    // 2. Sort in memory
    qsort(buffer, chunk->recordsInChunk, sizeof(Record), compareRecords);

    // 3. Write back to file
    for (int i = 0; i < chunk->recordsInChunk; i++) {
        CHUNK_UpdateIthRecord(chunk, i, buffer[i]);
    }

    free(buffer);
}

void sort_FileInChunks(int file_desc, int numBlocksInChunk) {
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, numBlocksInChunk);
    CHUNK chunk;
    
    while (CHUNK_GetNext(&iterator, &chunk) == 0) {
        sort_Chunk(&chunk);
    }
}
