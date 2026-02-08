#include <merge.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>

// Helper to check if a record is "smaller" or equal
bool isLessOrEqual(Record* r1, Record* r2) {
    // !shouldSwap(r1, r2) implies r1 <= r2
    return !shouldSwap(r1, r2); 
}

void merge(int input_FileDesc, int chunkSize, int bWay, int output_FileDesc) {
    CHUNK_Iterator chunkIterator = CHUNK_CreateIterator(input_FileDesc, chunkSize);
    CHUNK chunk;
    
    // Arrays to keep track of up to 'bWay' chunks
    CHUNK_RecordIterator* iterators = malloc(bWay * sizeof(CHUNK_RecordIterator));
    Record* currentRecords = malloc(bWay * sizeof(Record));
    bool* hasRecord = malloc(bWay * sizeof(bool)); // True if iterator has a valid record currently
    int activeChunks = 0;

    // Process the file in groups of 'bWay' chunks
    while (1) {
        // 1. Initialize iterators for the next batch of 'bWay' chunks
        activeChunks = 0;
        for (int i = 0; i < bWay; i++) {
            if (CHUNK_GetNext(&chunkIterator, &chunk) == 0) {
                iterators[i] = CHUNK_CreateRecordIterator(&chunk);
                // Fetch the first record of this chunk
                if (CHUNK_GetNextRecord(&iterators[i], &currentRecords[i]) == 0) {
                    hasRecord[i] = true;
                } else {
                    hasRecord[i] = false; // Empty chunk?
                }
                activeChunks++;
            } else {
                hasRecord[i] = false; // No more chunks
            }
        }

        if (activeChunks == 0) break; // Entire file processed

        // 2. K-way merge for this batch
        while (1) {
            int minIndex = -1;
            
            // Find the minimum record among the active chunks
            for (int i = 0; i < activeChunks; i++) {
                if (hasRecord[i]) {
                    if (minIndex == -1) {
                        minIndex = i;
                    } else {
                        if (isLessOrEqual(&currentRecords[i], &currentRecords[minIndex])) {
                            minIndex = i;
                        }
                    }
                }
            }

            if (minIndex == -1) break; // All chunks in this batch are exhausted

            // Write the smallest record to output
            HP_InsertEntry(output_FileDesc, currentRecords[minIndex]);

            // Advance the iterator that gave the smallest record
            if (CHUNK_GetNextRecord(&iterators[minIndex], &currentRecords[minIndex]) != 0) {
                hasRecord[minIndex] = false;
            }
        }
    }

    free(iterators);
    free(currentRecords);
    free(hasRecord);
}
