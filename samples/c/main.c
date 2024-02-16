#include <stdio.h>
#include "libvegas.h"

int main() {
    printf("hello\n");
    GoInt h = 0;
    h = NewProducer("test", 100, 10, 100);
    Send(h, "a", "a", 1);
    return 0;
}