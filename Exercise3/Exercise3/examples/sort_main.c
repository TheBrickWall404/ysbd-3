#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "merge.h"

#define RECORDS_NUM 500 // you can change it if you want
#define FILE_NAME "data.db"
#define OUT_NAME "out"




int createAndPopulateHeapFile(char* filename);

void sortPhase(int file_desc,int chunkSize);

void mergePhases(int inputFileDesc,int chunkSize,int bWay, int* fileCounter);

int nextOutputFile(int* fileCounter);

void verifySort(char* filename, int expectedTotalRecords) {
    int file_desc;
    if (HP_OpenFile(filename, &file_desc) < 0) {
        printf("Error: Could not open file %s for verification.\n", filename);
        return;
    }

    int lastBlock = HP_GetIdOfLastBlock(file_desc);
    int count = 0;
    Record prev, curr;
    bool first = true;
    bool sorted = true;

    printf("Verifying file: %s ...\n", filename);

    for (int bid = 1; bid <= lastBlock; bid++) {
        int recsInBlock = HP_GetRecordCounter(file_desc, bid);
        for (int i = 0; i < recsInBlock; i++) {
            HP_GetRecord(file_desc, bid, i, &curr);
            HP_Unpin(file_desc, bid); // Important: Unpin after reading

            if (!first) {
                // If prev > curr, then shouldSwap returns true, which means it's NOT sorted
                if (shouldSwap(&prev, &curr)) {
                    printf("[ERROR] Sort violation at record %d:\n", count);
                    printf("  Prev: %s %s\n", prev.name, prev.surname);
                    printf("  Curr: %s %s\n", curr.name, curr.surname);
                    sorted = false;
                    // You might want to break here or continue to find more errors
                }
            }
            prev = curr;
            first = false;
            count++;
        }
    }

    if (count != expectedTotalRecords) {
        printf("[ERROR] Data Loss! Expected %d records, found %d.\n", expectedTotalRecords, count);
        sorted = false;
    }

    if (sorted) {
        printf("[SUCCESS] The file is correctly sorted with %d records.\n", count);
    } else {
        printf("[FAILURE] The file is NOT correct.\n");
    }

    HP_CloseFile(file_desc);
}

int main() {
    int chunkSize = 5;
    int bWay = 4;
    int fileIterator = 0; // Initialize iterator to 0

    BF_Init(LRU);

    // 1. Create Data
    int file_desc = createAndPopulateHeapFile(FILE_NAME);
    
    // 2. Sort Phase
    sortPhase(file_desc, chunkSize);
    
    // 3. Merge Phase
    mergePhases(file_desc, chunkSize, bWay, &fileIterator);

    // 4. Verification
    // The last file created is "out" + (fileIterator - 1) + ".db"
    char finalFile[50];
    sprintf(finalFile, "out%d.db", fileIterator - 1);
    
    verifySort(finalFile, RECORDS_NUM);
    
    // Optional: Print the first few records to visually confirm
    /*
    int finalDesc;
    HP_OpenFile(finalFile, &finalDesc);
    HP_PrintBlockEntries(finalDesc, 1); 
    HP_CloseFile(finalDesc);
    */
}

int createAndPopulateHeapFile(char* filename){
  HP_CreateFile(filename);
  
  int file_desc;
  HP_OpenFile(filename, &file_desc);

  Record record;
  srand(12569874);
  for (int id = 0; id < RECORDS_NUM; ++id)
  {
    record = randomRecord();
    HP_InsertEntry(file_desc, record);
  }
  return file_desc;
}

/*Performs the sorting phase of external merge sort algorithm on a file specified by 'file_desc', using chunks of size 'chunkSize'*/
void sortPhase(int file_desc,int chunkSize){ 
  sort_FileInChunks( file_desc, chunkSize);
}

/* Performs the merge phase of the external merge sort algorithm  using chunks of size 'chunkSize' and 'bWay' merging. The merge phase may be performed in more than one cycles.*/
void mergePhases(int inputFileDesc, int chunkSize, int bWay, int* fileCounter){
  int outputFileDesc;
  int pass = 1; // Counter for passes

  while(chunkSize <= HP_GetIdOfLastBlock(inputFileDesc)){
    printf("Running Merge Pass %d (Chunk Size: %d, bWay: %d)...\n", pass, chunkSize, bWay);
    
    outputFileDesc = nextOutputFile(fileCounter);
    merge(inputFileDesc, chunkSize, bWay, outputFileDesc);
    HP_CloseFile(inputFileDesc);
    
    chunkSize *= bWay;
    inputFileDesc = outputFileDesc;
    pass++;
  }
  HP_CloseFile(outputFileDesc);
}

/*Creates a sequence of heap files: out0.db, out1.db, ... and returns for each heap file its corresponding file descriptor. */
int nextOutputFile(int* fileCounter){
    char mergedFile[50];
    char tmp[] = "out";
    sprintf(mergedFile, "%s%d.db", tmp, (*fileCounter)++);
    int file_desc;
    HP_CreateFile(mergedFile);
    HP_OpenFile(mergedFile, &file_desc);
    return file_desc;
}

