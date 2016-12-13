CXX = g++
CXXFLAGS = -std=c++11 #-g3
LDFLAGS += -L/usr/local/lib `pkg-config --libs grpc++ grpc`       \
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed \
           -lprotobuf -lpthread -ldl

PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`
PROTOS_PATH = .
vpath %.proto $(PROTOS_PATH)

EXE = cs426_graph_server

# space-separated list of header files
HDRS = mongoose.h headers.h test.grpc.pb.h test.pb.h

# space-separated list of source files
SRCS = mongoose.c hashtable.c checkpoint.c server.c

# automatically generated list of object files
OBJS = $(SRCS:.c=.o) test.pb.o test.grpc.pb.o tester_client.o tester_server.o

# default target
$(EXE): $(OBJS) $(HDRS)
	$(CXX) $^ $(CXXFLAGS) $(LDFLAGS) -o $@ #$(OBJS)

# dependencies
$(OBJS): $(HDRS)


.PRECIOUS: %.grpc.pb.cc
%.grpc.pb.cc: %.proto
        $(PROTOC) -I $(PROTOS_PATH) --grpc_out=. --plugin=protoc-gen-grpc=$(GRPC_CPP_PLUGIN_PATH) $<
.PRECIOUS: %.pb.cc
%.pb.cc: %.proto
        $(PROTOC) -I $(PROTOS_PATH) --cpp_out=. $<

# housekeeping
clean:
	rm -f core $(EXE) *.o
