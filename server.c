/*
* server.c
*
* by Stylianos Rousoglou
* and Alex Saiontz
*
* Provides the server functionality, including
* main, the request handler, and formatting json responses
*/

#include "mongoose.h"
#include "headers.h"

extern vertex_map map;// hashtable storing the graph
extern uint32_t generation;  // in-memory generation number
extern uint32_t tail;        // in-memory tail of the log

int fd;
int CHAIN_NUM;
char * IP_1;
char * IP_2;
char * IP_3;
char * NEXT_IP;
char * RPC_PORT;
pthread_mutex_t mt;

// Responds to given connection with code and length bytes of body
static void respond(struct mg_connection *c, int code, const int length, const char* body) {
  mg_send_head(c, code, length, "Content-Type: application/json");
  mg_printf(c, "%s", body);
}

// Respond with bad request
void badRequest(struct mg_connection *c) {
  respond(c, 400, 0, "");
}

// Finds the index of given key in an array of tokens
int argument_pos(struct json_token* tokens, const char* key) {
  struct json_token* ptr = tokens;
  int i = 0;
  while(strncmp(ptr[i].ptr, key, ptr[i].len)) i++;
  return i;
}

// Returns allocated string in json format, with one argument
char* make_json_one(const char* key, int key_length, int value) {
  // {} + "" + length of key + : + value + \0
  char dummy[20];
  sprintf(dummy, "%d", value);
  int value_length = strlen(dummy);
  int response_length = 2 + 2 + key_length + 1 + value_length + 1;
  char* response = (char *) malloc(sizeof(char) * response_length);
  sprintf(response, "{\"%s\":%d}",key,value);
  return response;
}

// Returns allocated string in json format, with two arguments
char* make_json_two(const char* key1, const char* key2, int key1_length, int key2_length, int value1, int value2) {
  // {} + "" + length of key1 + : + value + , + "" + length of key2 + : + value + \0
  char dummy1[20];
  char dummy2[20];
  sprintf(dummy1, "%d", value1);
  sprintf(dummy2, "%d", value2);
  int value1_length = strlen(dummy1);
  int value2_length = strlen(dummy2);

  int response_length = 2 + 2 + key1_length + value1_length + 5 + key2_length + value2_length + 1;
  char* response = (char*) malloc(sizeof(char) * response_length);
  sprintf(response, "{\"%s\":%d,\"%s\":%d}", key1, value1, key2, value2);
  return response;
}

// Function that returns array formatted as a C string
char* format_neighbors(uint64_t* neighbors, int size) {
  char* response = (char *) malloc(sizeof(char));
  int response_length = 1;
  response[0] = '[';
  char dummy[20];
  int dummy_length;
  int i;
  for(i = 0; i < size; i++) {
    if (i<size-1) sprintf(dummy, "%"PRIu64",", neighbors[i]);
    else sprintf(dummy, "%"PRIu64"", neighbors[i]);

    dummy_length = strlen(dummy);
    response_length += dummy_length;
    response = (char *) realloc(response, sizeof(char)*(response_length + 1));
    memcpy(response + response_length - dummy_length, dummy, dummy_length);
    response[response_length] = '\0';
  }
  response_length += 2;
  response = (char *) realloc(response, sizeof(char)*response_length);
  response[response_length-2] = ']';
  response[response_length-1] = '\0';
  return response;
}

// Returns allocated string in json format, formatted for get_neighbors
char* make_neighbor_response(const char* key1, const char* key2, int key1_length, int key2_length, int value1, char* neighbors) {
  char dummy[20];
  sprintf(dummy, "%d", value1);
  int value1_length = strlen(dummy);

  int response_length = 9 + key1_length + value1_length + key2_length + strlen(neighbors) + 1;
  char* response = (char *) malloc(sizeof(char) * response_length);
  sprintf(response, "{\"%s\":%d,\"%s\":%s}", key1, value1, key2, neighbors);
  return response;
}

// Event handler for request
static void ev_handler(struct mg_connection *c, int ev, void *p) {
  if (ev == MG_EV_HTTP_REQUEST) {
    struct http_message *hm = (struct http_message *) p;
    struct json_token* tokens = parse_json2(hm->body.p, hm->body.len);
    char* endptr;
    char* response;

    const char* arg_id = "node_id";
    const char* arg_a = "node_a_id";
    const char* arg_b = "node_b_id";
    struct json_token* find_id;
    struct json_token* find_a;
    struct json_token* find_b;

    // Sanity check for endpoint length and body not empty
    if (hm->uri.len < 16 || (tokens == NULL && strncmp(hm->uri.p, "/api/v1/checkpoint", hm->uri.len))) {
      badRequest(c);
      return;
    }
    if (!strncmp(hm->uri.p, "/api/v1/add_node", hm->uri.len)) {
      // body does not contain expected key
      if (find_id == 0) {
        badRequest(c);
        return;
      }
pthread_mutex_lock(&mt);
      // index of value
      int index1 = argument_pos(tokens, arg_id);
      uint64_t arg_int = strtoll(tokens[index1 + 1].ptr, &endptr, 10);

      if (arg_int% 3 != CHAIN_NUM-1){
        badRequest(c);
      }
      // returns true if successfully added
      else if (add_vertex(arg_int)) {
        response = make_json_one("node_id", 7, arg_int);
        respond(c, 200, strlen(response), response);
        free(response);

      } else {
        // vertex already existed
        respond(c, 204, 0, "");
      }
pthread_mutex_unlock(&mt);
    }
    else if (!strncmp(hm->uri.p, "/api/v1/add_edge", hm->uri.len)) {
      
      // body does not contain expected keys
      if (find_a == 0 || find_b == 0) {
        badRequest(c);
        return;
      }
      pthread_mutex_lock(&mt);

      // index of values
      int index1 = argument_pos(tokens, arg_a);
      int index2 = argument_pos(tokens, arg_b);
      uint64_t arg_a_int = strtoll(tokens[index1 + 1].ptr, &endptr, 10);
      uint64_t arg_b_int = strtoll(tokens[index2 + 1].ptr, &endptr, 10);

      // if this is the wrong partition, bad request
      if(!(arg_a_int %3 == CHAIN_NUM-1 || arg_b_int %3 == CHAIN_NUM-1 )){
         badRequest(c);
        pthread_mutex_unlock(&mt);
         return;
      }
      // if the node(s) that are supposed to be here are not, bad request
      if((arg_a_int %3 == CHAIN_NUM-1 && !(get_node(arg_a_int)))
        ||((arg_b_int %3 == CHAIN_NUM-1) && !(get_node(arg_b_int)))) {
         badRequest(c);
         pthread_mutex_unlock(&mt);
         return;
      }
      // here we know that any node supposed to be in this partition
      // is in fact here. So if both here, add edge and respond
      if(arg_a_int %3 == CHAIN_NUM-1 && arg_b_int %3 == CHAIN_NUM-1){
        switch (add_edge(arg_a_int, arg_b_int)) {
          case 400:
            respond(c, 400, 0, "");
          case 204:
            respond(c, 204, 0, "");
          case 200:
            response = make_json_two("node_a_id", "node_b_id", 9, 9, arg_a_int, arg_b_int);
            respond(c, 200, strlen(response), response);
            free(response);
        }
        pthread_mutex_unlock(&mt);
        return;
      }
      
      // which partition are the nodes in
      int arg_a_part = arg_a_int %3 +1;
      int arg_b_part = arg_b_int %3 +1;

      bool a_is_off = false;
      int in_graph_code;
      int add_code;
      
      // if arg_a is in another partition
      if (arg_a_part != CHAIN_NUM){
        a_is_off = true;
        // set next IP to the next partition's IP
        if (arg_a_part == 2){
          NEXT_IP = IP_2;
        }
        else NEXT_IP = IP_3;
        // does the node exist in the other partition?
        in_graph_code = send_to_next(GET_NODE, arg_a_int, 0);
        // node does not exist in the other partition, bad request
        if (in_graph_code == 400){
          respond(c, 400, 0, "");
          pthread_mutex_unlock(&mt);
          return;
        }
       // add node of current partition to next partition
       add_code = send_to_next(ADD_NODE, arg_b_int, 0);
//        if (add_code!=200){
//           // there is an error, fix it
//         }
       // add other partition's node to current partition
       add_vertex(arg_a_int);
      }
      else { // arb_b is in another partition
        if (arg_b_part == 2){
           NEXT_IP = IP_2;
        }
        else NEXT_IP = IP_3;
        // does the node exist in the other partition?
        in_graph_code = send_to_next(GET_NODE, arg_b_int, 0);
        // node does not exist in the other partition, bad request
        if (in_graph_code == 400){
          respond(c, 400, 0, "");
          pthread_mutex_unlock(&mt);
          return;
        }
        // add node of current partition to next partition
        add_code = send_to_next(ADD_NODE, arg_a_int, 0);
//         if (add_code!=200){
//           // there is an error, fix it
//         }
        // add other partition's node to current partition
        add_vertex(arg_b_int);

      }

      // send operation to other partition
      int code = send_to_next(ADD_EDGE, arg_a_int, arg_b_int);
      // if acknowledgment code not OK (=200), respond without writing
      if (code != 200) {
        respond(c, code, 0, "");
        pthread_mutex_unlock(&mt);
        return;
      }

      // if all is good, add the edge in the currect partition
      switch (add_edge(arg_a_int, arg_b_int)) {
        case 400:
        respond(c, 400, 0, "");
        case 204:
        respond(c, 204, 0, "");
        case 200:
        response = make_json_two("node_a_id", "node_b_id", 9, 9, arg_a_int, arg_b_int);
        respond(c, 200, strlen(response), response);
        free(response);
      }
pthread_mutex_unlock(&mt);
    }
    else if (!strncmp(hm->uri.p, "/api/v1/remove_edge", hm->uri.len)) {
      // body does not contain expected keys
      if (find_a == 0 || find_b == 0) {
        badRequest(c);
        return;
      }
pthread_mutex_lock(&mt);
      // index of values
      int index1 = argument_pos(tokens, arg_a);
      int index2 = argument_pos(tokens, arg_b);
      uint64_t arg_a_int = strtoll(tokens[index1 + 1].ptr, &endptr, 10);
      uint64_t arg_b_int = strtoll(tokens[index2 + 1].ptr, &endptr, 10);

      // if neither node is supposed to be here, bad request
      if(!(arg_a_int %3 == CHAIN_NUM-1 || arg_b_int %3 == CHAIN_NUM-1 )){
         badRequest(c);
pthread_mutex_unlock(&mt);
         return;
      }

      // if both nodes are here
      if(arg_a_int %3 == CHAIN_NUM-1 && arg_b_int %3 == CHAIN_NUM-1){
        if (remove_edge(arg_a_int, arg_b_int)) {
          response = make_json_two("node_a_id", "node_b_id", 9, 9, arg_a_int, arg_b_int);
          respond(c, 200, strlen(response), response);
          free(response);
        } else {
          respond(c, 400, 0, "");
        }
pthread_mutex_unlock(&mt);
        return;
      }
      int arg_a_part = arg_a_int %3 +1;
      int arg_b_part = arg_b_int %3 +1;
      
      // if arg_a is on the other partition
      if (arg_a_part != CHAIN_NUM){
        if (arg_a_part == 2){
          NEXT_IP = IP_2;
        }
        else NEXT_IP = IP_3;
       
      }
      else {
        if (arg_b_part == 2){
           NEXT_IP = IP_2;
        }
        else NEXT_IP = IP_3;
      }

      // send operation to next node
       int code = send_to_next(REMOVE_EDGE, arg_a_int, arg_b_int);
      // if acknowledgment code not OK (=200), respond without writing
      
      if (code != 200) {
        respond(c, code, 0, "");
pthread_mutex_unlock(&mt);
        return;
      }

      // if edge does not exist
      if (remove_edge(arg_a_int, arg_b_int)) {
        response = make_json_two("node_a_id", "node_b_id", 9, 9, arg_a_int, arg_b_int);
        respond(c, 200, strlen(response), response);
        free(response);
      } else {
        respond(c, 400, 0, "");
      }
pthread_mutex_unlock(&mt);
    }
    else if(!strncmp(hm->uri.p, "/api/v1/get_node", hm->uri.len)) {
      // body does not contain expected key
      if(find_id == 0) {
        badRequest(c);
        return;
      }
pthread_mutex_lock(&mt);
      // index of value
      int index1 = argument_pos(tokens, arg_id);
      long long arg_int = strtoll(tokens[index1 + 1].ptr, &endptr, 10);

      if (arg_int% 3 == CHAIN_NUM-1){
        bool in_graph = get_node(arg_int);
        response = make_json_one("in_graph", 8, in_graph);
        respond(c, 200, strlen(response), response);
        free(response);
      }
      else{
        badRequest(c);
      }
pthread_mutex_unlock(&mt);
    }
    else if(!strncmp(hm->uri.p, "/api/v1/get_edge", hm->uri.len)) {
      // body does not contain expected keys
      if(find_a == 0 || find_b == 0) {
        badRequest(c);
        return;
      }
      pthread_mutex_lock(&mt);
      // index of values
      int index1 = argument_pos(tokens, arg_a);
      int index2 = argument_pos(tokens, arg_b);
      long long arg_a_int = strtoll(tokens[index1 + 1].ptr, &endptr, 10);
      long long arg_b_int = strtoll(tokens[index2 + 1].ptr, &endptr, 10);

      int arg_a_part = arg_a_int %3 +1;
      int arg_b_part = arg_b_int %3 +1;

      bool a_in = false;
      bool b_in = false;
  
      // if arg_a should be on this partition
      if (arg_a_part == CHAIN_NUM){
        // if node does not exist, bad request
        if (!(get_node(arg_a_int))){
           badRequest(c);
           pthread_mutex_unlock(&mt);
           return;
        }
        // else, arg_a exists
        else a_in = true;
      }
      // if arg_b should be on this partition
      if (arg_b_part == CHAIN_NUM){
        // if node does not exist, bad request
        if (!(get_node(arg_b_int))){
           badRequest(c);
           pthread_mutex_unlock(&mt);
           return;
        }
        else b_in = true;
      }      

      if (arg_a_part != CHAIN_NUM){
        (arg_a_part == 2) ? (NEXT_IP = IP_2) : (NEXT_IP = IP_3);
       if(400 ==send_to_next(GET_NODE, arg_a_int, 0)){
        respond(c, 400, 0, "");
        pthread_mutex_unlock(&mt);
        return;
       }
       a_in = true;
     }
     if (arg_b_part !=CHAIN_NUM) {
        (arg_b_part == 2) ? (NEXT_IP = IP_2) : (NEXT_IP = IP_3);
       if(400 ==send_to_next(GET_NODE, arg_b_int, 0)){
        respond(c, 400, 0, "");
        pthread_mutex_unlock(&mt);
        return;
       }
       b_in = true;
     }
      if (!a_in || !b_in){
        respond(c, 400, 0, "");
        pthread_mutex_unlock(&mt);
        return;
      }
      
      bool in_graph = get_edge(arg_a_int, arg_b_int);
      response = make_json_one("in_graph", 8, in_graph);
      respond(c, 200, strlen(response), response);
      free(response);
      pthread_mutex_unlock(&mt);
    }
    else if(!strncmp(hm->uri.p, "/api/v1/get_neighbors", hm->uri.len)) {
      // body does not contain expected key
      if(find_id == 0) {
        badRequest(c);
        return;
      }
pthread_mutex_lock(&mt);
      // index of value
      int index1 = argument_pos(tokens, arg_id);
      long long arg_int = strtoll(tokens[index1 + 1].ptr, &endptr, 10);

      if (!get_node(arg_int) ) {
        respond(c, 400, 0, "");
      } else {
        int size;
        uint64_t *neighbors = get_neighbors(arg_int, &size);
        char* neighbor_array = format_neighbors(neighbors, size);
        response = make_neighbor_response("node_id", "neighbors", 7, 9, arg_int, neighbor_array);
        respond(c, 200, strlen(response), response);
        free(response);
        free(neighbors);
      }
pthread_mutex_unlock(&mt);
    }
    else {
      respond(c, 400, 0, "");
    }
  }
}

int main(int argc, char** argv) {
  //ensure correct number of arguments
  if (argc != 8) {
    fprintf(stderr, 
      "Usage: ./cs426_graph_server <graph_server_port> -p <partnum> -l <partlist> \n");
    return 1;
  }

  int cc;
  while ((cc = getopt (argc, argv, "p:l:")) != -1){
    switch (cc)
    {
      case 'p':
        CHAIN_NUM = atoi(optarg);
        break;
      case 'l':
        IP_1 = optarg;
        break;
      case '?':
        if (optopt == 'p')
          fprintf(stderr, "Option -%c requires an argument. \n", optopt);
        else if (isprint (optopt))
          fprintf(stderr, "Unknown option '-%c'.\n", optopt);
        else
          fprintf (stderr,
            "Unknown option character `\\x%x'.\n",
            optopt);
        return 1;
      default:
        abort ();
      }
  }
  if (argc - optind != 3) {
    fprintf(stderr, "Incorrect number of arguments\n");
    return 1;
  }
   
  char *s_http_port = argv[optind];
  IP_2 = argv[optind+1];
  IP_3 = argv[optind+2];


  
  fprintf(stderr, "Chain num is %d\n", CHAIN_NUM);

  // find the rpc port of the current vm
  if (CHAIN_NUM == 2){
   RPC_PORT = strchr(IP_2, ':');

  }
  if (CHAIN_NUM == 3){
   RPC_PORT = strchr(IP_3, ':');
  }
  struct mg_mgr mgr;
  struct mg_connection *c;

  // pass in void pointer
  mg_mgr_init(&mgr, NULL);

  c = mg_bind(&mgr, s_http_port, ev_handler);
  mg_set_protocol_http_websocket(c);

  map.nsize = 0;
  map.esize = 0;
  map.table = (vertex **) malloc(SIZE * sizeof(vertex*));

  int i;
  for (i = 0; i < SIZE; i++) (map.table)[i] = NULL;

  if (CHAIN_NUM != 1) {
    // create reference to second thread
    pthread_t inc_x_thread;
    int x;

    fprintf(stderr, "In chain %d\n", CHAIN_NUM);
    if(pthread_create(&inc_x_thread, NULL, serve_rpc, &x)) {
      fprintf(stderr, "Error creating thread\n");
      return 1;
    }
  }

  for (;;) {
    mg_mgr_poll(&mgr, 1000);
  }
  mg_mgr_free(&mgr);

  return 0;
}
