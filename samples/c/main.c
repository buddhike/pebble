#include <stdio.h>
#include <string.h>
#include "libvegas.h"

int main() {
    int h = 0;
    char* err;
    char* msg = "hello from c";
    ProducerConfig c = NewProducerConfig();
    err = NewProducer("test", c, &h);
    if (err != NULL) {
        printf("%s\n", err);
        return 1;
    }

    Send(h, "c", msg, strlen(msg));
    return 0;
}