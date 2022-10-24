#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <string.h>
#include <math.h>
#include <stdbool.h>
#include "file.h"
#include "time.h"

const char* FILENAME = "./huge_int_set";
// const char* FILENAME = "./small_int_set";
const char* INDEXFILE = "./index";

struct avgNums {
    int idx;
    off_t current;
    off_t limit;
};

struct retNums {
    long double n;
    long double avg;
};

int threadCount = 10;

void* readAvg(void* arg) {
    struct avgNums* p =(struct avgNums*)arg;

    long double _av = 0.0;
    long double _n = 1.0;

    FILE* f0 = fopen(FILENAME, "rb");
    fseek(f0, p->current, 0);
    long long cummulativeSize = 0;
    while (1) {
        if (cummulativeSize >= p->limit) break;
        char b[18];
        b[0] = '\0';
        fgets(b, 18, f0);
        if (strlen(b) == 0) {
            break;
        }
        cummulativeSize += strlen(b);
        long double f = (long double) atoll(b);
        _av = (_n-1.0)/_n*_av + f/_n;
        _n += 1.0;
        if (feof(f0)) break;
    }
    fclose(f0);

    free(p);
    _n--;

    struct retNums* ret = malloc(sizeof(struct retNums));
    ret->n = _n;
    ret->avg = _av;

    return (void*) ret;

}

void buildIndex(int rc) {
    struct stat st;
    stat(FILENAME, &st);
    off_t totalSize = st.st_size;
    off_t blockSize = (off_t)ceill((long double)totalSize / (long double)rc);

    FILE* file = fopen(FILENAME, "r");

    off_t sp[rc+1]; // 先分配这么长，最大长度不会超过 rc+1
    sp[0] = 0;
    int count = 1;

    off_t current = blockSize;
    for (int i = 1; i < rc; i++) {
        fseek(file, current, 0);
        off_t offset = 0;
        bool hasReturn = false;
        // 读一个字节
        while (1) {
            int c = fgetc(file);
            offset++;
            if (c == EOF) {
                break;
            }
            if (c == '\n') {
                // 如果读到一个回车，直接记录上一个的结束位置
                count++;
                sp[count-1] = current + offset;
                hasReturn = true;
                break;
            }
        }
        if (!hasReturn) {
            // 一行到最后都没读到回车，也算是一行
            count++;
            sp[count-1] = current + offset + 1;
        }
        current += offset + blockSize + 1;
        if (current >= totalSize) {
            break;
        }
    }

    count++;
    sp[count-1] = totalSize;
    fclose(file);

    FILE* f0 = fopen(INDEXFILE, "w");
    for (int i = 0; i < count; i++) {
        char c0[18];
        sprintf(c0, "%lld\n", sp[i]);
        fwrite(c0, strlen(c0),1, f0);
    }
    fflush(f0);
    fclose(f0);
}

int main(int argc, char** argv) {
    if (argc == 3) {
        if (strcmp(argv[1],"index") == 0) {
            int rc = atoi(argv[2]);
            buildIndex(rc);
        }
        return 0;
    }
    if (argc == 2) {
        threadCount = atoi(argv[1]);
    }

    bool useIndex = false;
    off_t indexes[1024];

    if (access(INDEXFILE, 0) == 0) {
        // 如果索引文件存在
        off_t _indexes[1024];
        int cnt = 0;
        readIndex(INDEXFILE, &cnt, _indexes);
        useIndex = true;
        threadCount = cnt - 1;
        for (int i = 0; i < cnt; i++) {
            indexes[i] = _indexes[i];
        }
    }

    struct stat st;
    stat(FILENAME, &st);
    off_t totalSize = st.st_size;
    off_t blockSize = (off_t)ceill((long double)totalSize / (long double)threadCount);

    long long _start = currentTimeMillis();

    pthread_t tids[threadCount];

    off_t current = 0;
    off_t limitSize = 0;

    for (int i = 0; i < threadCount; i++) {
        if (useIndex) {
            limitSize = indexes[i+1] - indexes[i];
        } else {
            limitSize = blockSize;
        }
        if (limitSize <= 0) {
            break;
        }
        struct avgNums* p = malloc(sizeof(struct avgNums));
        p->idx = i;
        p->current = current;
        p->limit = limitSize;
        pthread_create(&tids[i], NULL, readAvg, (void *)p);
        if (useIndex) {
            current = indexes[i+1];
        } else {
            current += blockSize + 1;
        }
    }

    long double av = 0.0;
    long double n = 1.0;

    for (int i = 0; i < threadCount; i++) {
        // 等所有线程结束
        struct retNums* ret;
        pthread_join(tids[i], (void**)&ret);
        // 计算每一组的平均值
        av = (n-1.0)/(n+ret->n-1.0)*av + ret->avg*(ret->n/(n+ret->n-1.0));
        n += ret->n;
        free(ret);
    }

    // n--;

    long long _end = currentTimeMillis();

    printf("avg = %.12Lf, time = %lld\n", av, _end - _start);
    return 0;
}
