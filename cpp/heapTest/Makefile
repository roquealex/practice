CXXFLAGS =	-std=c++11 -g -Wall -fmessage-length=0

OBJS =		heapTest.o HeapBase.o

LIBS =

TARGET =	heapTest

$(TARGET):	$(OBJS)
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS)

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
