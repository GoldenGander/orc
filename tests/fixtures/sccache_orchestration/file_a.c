#include <stdio.h>

int add(int a, int b) {
    return a + b;
}

int main(void) {
    printf("a: %d\n", add(1, 2));
    return 0;
}
