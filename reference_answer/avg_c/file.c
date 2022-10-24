#include "file.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void readIndex(const char* filename, int* count, off_t* indexes) {
    FILE* file = fopen(filename, "r");
    int c = 0;
    while (1) {
        char c0[18];
        c0[0] = '\0';   // 防止未覆盖的情况
        fgets(c0, 18, file);
        if (strlen(c0) == 0) {
            break;
        }
        off_t d = atoll(c0);
        c++;
        indexes[c-1] = d;
        if (feof(file) != 0) {
            break;
        }
    }
    fclose(file);
    *count = c;
}