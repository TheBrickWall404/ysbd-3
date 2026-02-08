#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "chunk.h"

CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk) {
    CHUNK_Iterator iterator;
    iterator.file_desc = fileDesc;
    iterator.current = 1; // Heap files start data at block 1
    iterator.blocksInChunk = blocksInChunk;
    iterator.lastBlocksID = HP_GetIdOfLastBlock(fileDesc);
    return iterator;
}

int CHUNK_GetNext(CHUNK_Iterator *iterator, CHUNK* chunk) {
    if (iterator->current > iterator->lastBlocksID) {
        return -1; // No more chunks
    }

    chunk->file_desc = iterator->file_desc;
    chunk->from_BlockId = iterator->current;

    // Calculate end block, ensuring we don't exceed the file's last block
    int potentialEnd = iterator->current + iterator->blocksInChunk - 1;
    if (potentialEnd > iterator->lastBlocksID) {
        chunk->to_BlockId = iterator->lastBlocksID;
    } else {
        chunk->to_BlockId = potentialEnd;
    }

    chunk->blocksInChunk = chunk->to_BlockId - chunk->from_BlockId + 1;

    // Count records in this chunk
    chunk->recordsInChunk = 0;
    for (int i = chunk->from_BlockId; i <= chunk->to_BlockId; i++) {
        chunk->recordsInChunk += HP_GetRecordCounter(chunk->file_desc, i);
    }

    // Update iterator for next call
    iterator->current = chunk->to_BlockId + 1;
    return 0;
}

int CHUNK_GetIthRecordInChunk(CHUNK* chunk, int i, Record* record) {
    int count = 0;
    // Iterate through blocks in the chunk to find the one containing the i-th record
    for (int blk = chunk->from_BlockId; blk <= chunk->to_BlockId; blk++) {
        int recordsInBlock = HP_GetRecordCounter(chunk->file_desc, blk);
        
        if (i < count + recordsInBlock) {
            // Found the block
            int cursor = i - count;
            if (HP_GetRecord(chunk->file_desc, blk, cursor, record) == 0) {
                HP_Unpin(chunk->file_desc, blk); // Unpin after reading
                return 0;
            }
            return -1; 
        }
        count += recordsInBlock;
    }
    return -1; // Index out of bounds
}

int CHUNK_UpdateIthRecord(CHUNK* chunk, int i, Record record) {
    int count = 0;
    for (int blk = chunk->from_BlockId; blk <= chunk->to_BlockId; blk++) {
        int recordsInBlock = HP_GetRecordCounter(chunk->file_desc, blk);
        
        if (i < count + recordsInBlock) {
            int cursor = i - count;
            if (HP_UpdateRecord(chunk->file_desc, blk, cursor, record) == 0) {
                HP_Unpin(chunk->file_desc, blk); // Unpin after updating
                return 0;
            }
            return -1;
        }
        count += recordsInBlock;
    }
    return -1;
}

void CHUNK_Print(CHUNK chunk) {
    printf("Chunk: FileDesc %d, Blocks %d-%d, Records %d\n", 
           chunk.file_desc, chunk.from_BlockId, chunk.to_BlockId, chunk.recordsInChunk);
    Record rec;
    for (int i = 0; i < chunk.recordsInChunk; i++) {
        CHUNK_GetIthRecordInChunk(&chunk, i, &rec);
        printRecord(rec);
    }
}

CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk) {
    CHUNK_RecordIterator iterator;
    iterator.chunk = *chunk;
    iterator.currentBlockId = chunk->from_BlockId;
    iterator.cursor = 0;
    return iterator;
}

int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator, Record* record) {
    while (iterator->currentBlockId <= iterator->chunk.to_BlockId) {
        int recordsInBlock = HP_GetRecordCounter(iterator->chunk.file_desc, iterator->currentBlockId);
        
        if (iterator->cursor < recordsInBlock) {
            if (HP_GetRecord(iterator->chunk.file_desc, iterator->currentBlockId, iterator->cursor, record) == 0) {
                HP_Unpin(iterator->chunk.file_desc, iterator->currentBlockId);
                iterator->cursor++;
                return 0; // Success
            }
            return -1; // Error reading
        } else {
            // Move to next block
            iterator->currentBlockId++;
            iterator->cursor = 0;
        }
    }
    return -1; // End of chunk
}
