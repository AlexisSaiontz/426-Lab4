#include <iostream>
#include <memory>
#include <string>
#include <unistd.h>
#include <grpc++/grpc++.h>

#include "test.grpc.pb.h"
#include "headers.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

using mutate::Node;
using mutate::Edge;
using mutate::Code;
using mutate::Mutator;

extern int CHAIN_NUM;
extern char* NEXT_IP;
extern char* RPC_PORT;

// define the service class.
class TesterService final : public Mutator::Service {

  Status add_node(ServerContext* context, const Node* node,
    Code* reply) override {
      bool result;
      int r_code;
      // Apply change and reply
      result = add_vertex(node->id());
      if (result) {
        printf("Added node %d\n", (int) node->id());
        reply->set_code(200);
      } else {
        reply->set_code(204);
      }
      return Status::OK; 
    }
  

    Status remove_node(ServerContext* context, const Node* node,
      Code* reply) override {
      // this won't be used so get rid of it
      }

      Status add_edge_alt(ServerContext* context, const Edge* edge,
        Code* reply) override {
          printf("Received: Add edge %d - %d\n", (int) edge->id_a(), (int) edge->id_b());
          int result;
          int r_code;   
          // Apply change and reply
          result = add_edge(edge->id_a(), edge->id_b());
          if (result==200) {
            printf("Added edge %d, %d\n", (int) edge->id_a(), (int) edge->id_b());
            reply->set_code(200);
          } else {
            reply->set_code(result);
          }
          return Status::OK;
        }
        Status remove_edge_alt(ServerContext* context, const Edge* edge,
          Code* reply) override {
            printf("Received: Remove edge %d - %d\n", (int) edge->id_a(), (int) edge->id_b());
            bool result;
            int r_code;
            if(!get_node(edge->id_a())){
              printf("no node: %d\n", (int) edge->id_a());
            }
            if(!get_node(edge->id_b())){
              printf("no node: %d\n", (int) edge->id_b());
            }

            // Apply change and reply
            result = remove_edge(edge->id_a(), edge->id_b());
            if (result) {
              printf("Removed edge %d, %d\n", (int) edge->id_a(), (int) edge->id_b());
              reply->set_code(200);
            } else {
              reply->set_code(400);
            }
            return Status::OK;
            
          }
        Status get_node_alt(ServerContext* context, const Node* node,
          Code* reply) override {
            bool result;
            int r_code;
       
            // Apply change and reply
            result = get_node(node->id());
            if (result) {
              reply->set_code(200);
            } else {
              reply->set_code(400);
            }
            return Status::OK;
          }
               
        };

        void *serve_rpc(void *arg) {

          // server serves on its own IP, port 50051

          char * address= (char*)malloc(sizeof(char) *(7+strlen(RPC_PORT)));
          strcpy(address, "0.0.0.0");
          strcat(address, RPC_PORT);
        

          TesterService service;

          ServerBuilder builder;
          // Listen on the given address without any authentication mechanism.
          builder.AddListeningPort(address, grpc::InsecureServerCredentials());
          // Register "service" as the instance through which we'll communicate with
          // clients. In this case it corresponds to an *synchronous* service.
          builder.RegisterService(&service);
          // Finally assemble the server.
          std::unique_ptr<Server> server(builder.BuildAndStart());
          std::cout << "Server listening on " << address << std::endl;
          // Wait for the server to shutdown. Note that some other thread must be
          // responsible for shutting down the server for this call to ever return.
          server->Wait();
        }
      
