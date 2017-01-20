#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <unistd.h>

int main(int argc, char **argv) {
    char *mem;
    unsigned long bytes;

    if (argc != 2) {
        fprintf(stderr, "Usage: %s <bytes>\n", argv[0]);
        exit(1);
    }

    bytes = atoll(argv[1]);

    mem = malloc(bytes);
    if (!mem) {
        fprintf(stderr, "Failed memory allocation!\n");
        exit(1);
    }

    if (mlock(mem, bytes) < 0) {
        fprintf(stderr, "Failed to lock mem pages!\n");
        exit(1);
    }

    pause();

    free(mem);
    exit(0);
}
