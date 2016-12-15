#include <iostream>
#include <memory>
#include <string>
#include <grpc++/grpc++.h>
#include <stdint.h>

#include "test.grpc.pb.h"
#include "headers.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

using mutate::Node;
using mutate::Edge;
using mutate::Code;
using mutate::Mutator;

extern int CHAIN_NUM;
extern char* NEXT_IP;



class MutatorClient {
public:
  MutatorClient(std::shared_ptr<Channel> channel)
  : stub_(Mutator::NewStub(channel)) {}

  int add_node(const uint64_t id) {
    Node toAdd;
    toAdd.set_id(id);

    Code code;

    ClientContext context;
    // The actual RPC.
    Status status = stub_->add_node(&context, toAdd, &code);

    // Act upon its status.
    if (status.ok()) {
      std::cout << "Code returned: " << code.code() << std::endl;
      return code.code();
    } else {
      std::cout <<  "RPC failed" << std::endl;
      return 500;
    }
  }
  int remove_node(const uint64_t id) {
    Node toRem;
    toRem.set_id(id);

    Code code;

    ClientContext context;
    // The actual RPC.
    Status status = stub_->remove_node(&context, toRem, &code);

    // Act upon its status.
    if (status.ok()) {
      std::cout << "Code returned: " << code.code() << std::endl;
      return code.code();
    } else {
      std::cout <<  "RPC failed" << std::endl;
      return 500;
    }
  }
  int add_edge(const uint64_t id_a, const uint64_t id_b) {
    Edge toAdd;
    toAdd.set_id_a(id_a);
    toAdd.set_id_b(id_b);

    Code code;

    ClientContext context;
    // The actual RPC.
    Status status = stub_->add_edge_alt(&context, toAdd, &code);

    // Act upon its status.
    if (status.ok()) {
      std::cout << "Code returned: " << code.code() << std::endl;
      return code.code();
    } else {
      std::cout <<  "RPC failed" << std::endl;
      return 500;
    }
  }
  int remove_edge(const uint64_t id_a, const uint64_t id_b) {
    Edge toRem;
    toRem.set_id_a(id_a);
    toRem.set_id_b(id_b);

    Code code;

    ClientContext context;
    // The actual RPC.
    Status status = stub_->remove_edge_alt(&context, toRem, &code);

    // Act upon its status.
    if (status.ok()) {
      std::cout << "Code returned: " << code.code() << std::endl;
      return code.code();
    } else {
      std::cout <<  "RPC failed" << std::endl;
      return 500;
    }
  }
  int get_node(const uint64_t id) {
    Node toGet;
    toGet.set_id(id);

    Code code;

    ClientContext context;
    // The actual RPC.
    Status status = stub_->get_node_alt(&context, toGet, &code);
    // if code == 200, it is in the graph. if it is 400, it is not in the graph.
    // Act upon its status.
    if (status.ok()) {
      std::cout << "Code returned: " << code.code() << std::endl;
      return code.code();
    } else {
      std::cout <<  "RPC failed" << std::endl;
      return 500;
    }
  }
private:
  std::unique_ptr<Mutator::Stub> stub_;

};

int send_to_next(const uint64_t opcode, const uint64_t id_a, const uint64_t id_b) {
  // Instantiate the client. It requires a channel, out of which the actual RPCs
  // are created. This channel models a connection to an endpoint
  // We indicate that the channel isn't authenticated
  // (use of InsecureChannelCredentials()).

  MutatorClient mutator(grpc::CreateChannel(
    (NEXT_IP), grpc::InsecureChannelCredentials()));
    int code;
    switch(opcode) {
      case ADD_NODE:
      code = mutator.add_node(id_a);
      break;
      case REMOVE_NODE:
      code = mutator.remove_node(id_a);
      break;
      case ADD_EDGE:
      code = mutator.add_edge(id_a, id_b);
      break;
      case REMOVE_EDGE:
      code = mutator.remove_edge(id_a, id_b);
      break;
      case GET_NODE:
      code = mutator.get_node(id_a);
      break;
    }

    std::cout << "Client received status code: " << code << std::endl;
    return code;
  }
