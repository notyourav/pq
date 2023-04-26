TARGET=pq
SRCS=main.c pq.c
OBJECTS=$(SRCS:.c=.o)

$(TARGET): $(OBJECTS)
	gcc -g -Wall -o $@ $^

$(OBJECTS): $(SRCS)
	gcc -g -Wall -c $(SRCS)

clean:
	rm -f *.gch *.o $(TARGET)
