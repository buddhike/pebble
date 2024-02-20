#include <stdio.h>
#include "libvegas.h"

int main() {
    printf("hello\n");
    GoInt h = 0;
    ProducerConfig c = NewProducerConfig("test");
    h = NewProducer(c);
    Send(h, "c", "a", 1);
    return 0;
}