#include <stdio.h>
#include "libvegas.h"

int main() {
    printf("hello\n");
    GoInt h = 0;
    ProducerConfig c = NewProducerConfig();
    h = NewProducer("test", c);
    Send(h, "c", "a", 1);
    return 0;
}