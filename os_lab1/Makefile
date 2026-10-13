CC := gcc
override CFLAGS += -O3 -Wall

SOURCE := lab1.c
BINARY := lab1

all: $(BINARY)

$(BINARY): $(SOURCE) $(patsubst %.c, %.h, $(SOURCE))
	$(CC) $(CFLAGS) $< -o $@

.PHONY: clean
clean:
	rm -f $(BINARY)
