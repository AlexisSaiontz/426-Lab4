/*
 * hashtable.c 
 *  
 * by Stylianos Rousoglou
 * and Alex Saiontz
 *
 * Provides hashtable, linked list, and
 * queue functionality, as well as a 
 * shortest path alogirthm
 */

#include "headers.h"


/*
	Hashtable API
*/

// global hashtable for vertices
vertex_map map;


// Returns hash value
int hash_vertex(uint64_t id) {
	return id % SIZE;
}

// Returns pointer to vertex id, or NULL if it doesn't exist
vertex *ret_vertex(uint64_t id) {
	int hash = hash_vertex(id);
	vertex** table = map.table;
	vertex* index = table[hash];

	if(index != NULL) {
		if(index->id == id) return index;
		while(index->next) {
			if(index->id == id) return index;
			index = index->next;
		}
	}
	return NULL;
}

// Adds vertex, returns false is vertex existed
bool add_vertex(uint64_t id) {

	

	int hash = hash_vertex(id);
	vertex** table = map.table;

	// ensures vertex does not exist
	if(ret_vertex(id)) return false;

	vertex* new = malloc(sizeof(vertex));
	if(!new) exit(1); // TODO: free everything
	new->id = id;
	new->next = table[hash];
	new->head = NULL;
	new->path = -1;
	new->visited = 0;
	table[hash] = new;
	map.nsize += 1;



	return true;
}

// Check if a vertex is in a graph. 
bool get_node(uint64_t id) {
	if (ret_vertex(id) == NULL){
		return false;
	}
	return true;
}

// Check if an edge is in a graph 
bool get_edge(uint64_t a, uint64_t b){
	vertex *v1 = ret_vertex(a);
	vertex *v2 = ret_vertex(b);
	if (LL_contains(&(v1->head), b) && LL_contains(&(v2->head), a)){
		return true;
	}
	return false;
}

/*
	Linked-list API
*/

// Inserts n into linked list
void LL_insert(edge** head, uint64_t n){
    edge* newnode = malloc(sizeof(edge));
    newnode->b = n;
    newnode->next = NULL;

    if(*head==NULL)
    {
        *head = newnode;
    }
    else
    {
        edge* temp; 
        temp = *head;
        *head = newnode;
        newnode->next = temp;
    }
}

// Returns true if n is in the linked list
bool LL_contains(edge** head, uint64_t n) {
    edge* ptr = *head; 
    while(ptr) {
        if(ptr->b == n) return true;
        ptr = ptr->next;
    }
    return false;
}

// Removes n from linked list
bool LL_delete(edge** head, uint64_t n)
{
    if(*head == NULL) return false;
    else {
        edge* tmp = *head;
        if(tmp->b == n) {
            tmp = tmp->next;
            free(*head);
            *head = tmp;
            return true;
        }

        while(tmp->next)
        {
            if(tmp->next->b == n) {
                edge* tmp2 = tmp->next->next;
                free(tmp->next);
                tmp->next = tmp2;
                return true;
            }
            tmp = tmp->next;
        }
    }
    return false;
}

// Adds edge, returns 
int add_edge(uint64_t a, uint64_t b) {

	vertex** table = map.table;

	vertex* v1 = table[hash_vertex(a)];
	vertex* v2 = table[hash_vertex(b)];

	// find vertex a
	while(v1) {
		if (v1->id == a) break;
		v1 = v1->next;
	}
	// find vertex b
	while(v2) {
		if (v2->id == b) break;
		v2 = v2->next;
	}
	// code 400
	if(!v1 || !v2 || a == b){

		 return 400;
	}
	if(LL_contains(&(v1->head), b)) {

		return 204;
	}
	LL_insert(&(v1->head), b);
	LL_insert(&(v2->head), a);
	map.esize += 1;
	return 200;
}

// Removes edge, returns false if it didn't exist
bool remove_edge(uint64_t a, uint64_t b) {
	vertex** table = map.table;
	
	vertex* v1 = table[hash_vertex(a)];
	vertex* v2 = table[hash_vertex(b)];

	// find vertex a
	while(v1) {
		if (v1->id == a) break;
		v1 = v1->next;
	}
	// find vertex b
	while(v2) {
		if (v2->id == b) break;
		v2 = v2->next;
	}

	// can't remove edge
	if(!v1 || !v2) {

		return false;
	}
	map.esize -= 1;
	return LL_delete(&(v1->head), b) && LL_delete(&(v2->head), a);
}

/*
	Queue API
*/

// Initializes queue
queue * queueCreate(void){
	queue *q;
	q = malloc(sizeof(queue));
	q->head = q->tail = 0;
	return q;

}

// Enqueues element value to queue *q
void enqueue(queue **q, uint64_t value){
	struct elt *e;
	e = malloc(sizeof(struct elt));
	assert(e);
	e->value = value;
	e->next = 0;
	if((*q)->head == 0) {
		(*q)->head = e;
	}
	else {
		(*q)->tail->next = e;
	}
	(*q)->tail = e;
}

// Dequeues element value from queue *q
uint64_t dequeue(queue **q){
	uint64_t ret;
	struct elt *e;
	assert(!((*q)->head == 0));

	ret= (*q)->head->value;

	e = (*q)->head;
	(*q)->head = e->next;
	free(e);
	return ret;

}

// Empties queue and frees allocated memory
void queue_destroy(queue **q){
	while(!((*q)->head == 0)){
		dequeue(q);

	}
	free(*q);
}

/*
	Other operations
*/
// Given a valid node_id, returns list of neighbors
uint64_t *get_neighbors(uint64_t id, int* n){
	vertex *v = ret_vertex(id);
	int size = 0;
	int i;
	
	edge *runner;
	runner = v->head;
	while(runner){
		size++;
		runner = runner->next;
	}	

	uint64_t *neighbors = malloc(sizeof(uint64_t)*size);

	runner = v->head;
	for (i=0; i<size; i++){
		neighbors[i] = runner->b;
		runner = runner->next;
	}
	*n = size;
	return neighbors;
}
