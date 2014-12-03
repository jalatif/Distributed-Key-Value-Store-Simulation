/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
    this->transaction_id = 0; //set transaction id to begin with 0 for this node
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
*
* Short print function for printing address. For debugging purpose only.
*/
void MP2Node::printAddress(Address *addr) {
    printf("%d.%d.%d.%d:%d ", addr->addr[0], addr->addr[1], addr->addr[2], addr->addr[3], *(short *)&addr->addr[4]);
}
/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());

    //check if stablization required or not by checking the change in ring
    change = isRingSame(curMemList);

    //If the ring has changed or it was empty before assign ring the new Member List
    if (ring.empty() || change)
        ring = curMemList;

    //Initially when hasMyReplicas and haveReplicasOf is empty initialize those variables for this node
    if (hasMyReplicas.empty() || haveReplicasOf.empty())
        assignReplicationNodes();

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
	//Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring

    cout << "Stablization required = " << change << endl;

    // If stablization is required run stablization protocol.
    if (change && !ht->isEmpty())
        stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
    /*
     * Hash the key and modulo by number of members in the ring
     * Find the Node where that key should be present
     * Also, replicate that key on the next 2 neighbors
     * Message all those nodes to save this key
     */

    // find the destination 3 nodes where this key needs to go
    vector<Node> destination_nodes = findNodes(key);

    // print the location of nodes just for debugging
    for(int i = 0; i < destination_nodes.size(); i++){
        cout << "Key = " << key << " Replica = ";
        printAddress(destination_nodes[i].getAddress());
        cout << endl;
    }

    // Create a create message with key and value and replica type and send each replica create message according to its type
    Message *msg;
    ReplicaType type;
    for (int i = 0; i < RF; i++){ // Loop over each replica
        type = static_cast<ReplicaType>(i); // convert int type into enum ReplicaType
        msg = new Message(this->transaction_id, getMemberNode()->addr, CREATE, key, value, type); //create a create type message
        emulNet->ENsend(&getMemberNode()->addr, destination_nodes[i].getAddress(), msg->toString());
        free(msg); // free message
    }
    // Increment this node transaction_id and set other transaction details like key, value and MessageType
    initTransactionCount(this->transaction_id++, key, value, CREATE);
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
    // find the destination 3 nodes where this key needs to go
    vector<Node> destination_nodes = findNodes(key);

    // Create a read message with key and send each replica this message
    Message *msg;
    for (int i = 0; i < RF; i++){
        msg = new Message(this->transaction_id, getMemberNode()->addr, READ, key);
        emulNet->ENsend(&getMemberNode()->addr, destination_nodes[i].getAddress(), msg->toString());
        free(msg);
    }
    // Increment this node transaction_id and set other transaction details like key, value and MessageType
    initTransactionCount(this->transaction_id++, key, "", READ);

}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */

    //find the destination 3 nodes where this key needs to go
    vector<Node> destination_nodes = findNodes(key);

    // Create a update message with key and value and send each replica this message according to their type
    Message *msg;
    ReplicaType type;
    for (int i = 0; i < RF; i++){
        type = static_cast<ReplicaType>(i);
        msg = new Message(this->transaction_id, getMemberNode()->addr, UPDATE, key, value, type);
        emulNet->ENsend(&getMemberNode()->addr, destination_nodes[i].getAddress(), msg->toString());
        free(msg);
    }
    // Increment this node transaction_id and set other transaction details like key, value and MessageType
    initTransactionCount(this->transaction_id++, key, value, UPDATE);
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */

    //find the destination 3 nodes where this key needs to go
    vector<Node> destination_nodes = findNodes(key);

    // Create a delete message with key and send each replica this message
    Message *msg;
    for (int i = 0; i < RF; i++){
        msg = new Message(this->transaction_id, getMemberNode()->addr, DELETE, key);
        emulNet->ENsend(&getMemberNode()->addr, destination_nodes[i].getAddress(), msg->toString());
        free(msg);
    }
    // Increment this node transaction_id and set other transaction details like key, value and MessageType
    initTransactionCount(this->transaction_id++, key, "", DELETE);
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
    // Use entry class object to convert the information into string
    Entry e(value, par->getcurrtime(), replica);
    return ht->create(key, e.convertToString()); //insert key and value in local hashtable and return the status
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value

    return ht->read(key); // read the value in local hashtable and return the value
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 * */
	// Update key in local hash table and return true or false
    // Use entry class object to convert the information into string
    Entry e(value, par->getcurrtime(), replica);
    return ht->update(key, e.convertToString()); // update key and value in local hashtable and return the status
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 *
	 * */
	// Delete the key from the local hash table
    return ht->deleteKey(key); // delete the key in local hashtable and return the status
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */

        cout << "Got message = " <<  message << endl;

        Message incoming_msg(message); //convert the message in string form to Message type

        //check for message type and call their handlers accordingly.
        switch(incoming_msg.type) {
            case CREATE     : {processCreate(incoming_msg); break;}
            case READ       : {processRead(incoming_msg); break;}
            case REPLY      : {processNodesReply(incoming_msg); break;}
            case READREPLY  : {processReadReply(incoming_msg);break;}
            case DELETE     : {processDelete(incoming_msg); break;}
            case UPDATE     : {processUpdate(incoming_msg); break;}
            default         : {}
        }
	}

    // At the end check the acks or nacks that this node may have received from the reply messages from the nodes to
    // whom this node may sent the client CRUD message
    checkReplyMessages();

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

// comparator that sorts the entry object by timestamp in descending order.
struct by_entry_timestamp {
    bool operator()(pair<int, string> const &a,  pair<int, string> const &b) const {
        Entry e1(a.second);
        Entry e2(b.second);
        return e1.timestamp > e2.timestamp;
    }
};

//pass the transaction count map iterator to this function to log the success according to the message type
// These log messages are the messages of coordinator
void MP2Node::logCoordinatorSuccess(map<int, transaction_details>::iterator it) {
    switch(this->transaction_count[it->first].rep_type){
        case CREATE : {log->logCreateSuccess(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
        case DELETE : {log->logDeleteSuccess(&getMemberNode()->addr, true, it->first, it->second.key); break;}
        case READ   : {
            if (!it->second.value.empty()){ // print Sucess only when value returned is not invalid i.e. ""
                // Sort the value by timestamp and return the value with highest timestamp.
                sort(transaction_count[it->first].ackStore.begin(), transaction_count[it->first].ackStore.end(), by_entry_timestamp());
                log->logReadSuccess(&getMemberNode()->addr, true, it->first, it->second.key, transaction_count[it->first].ackStore[0].second);
            }
            else // otherwise log failure
                log->logReadFail(&getMemberNode()->addr, true, it->first, it->second.key);
            break;
        }
        case UPDATE : {log->logUpdateSuccess(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
        default: {}
    }
    cout << "Success transaction  for transaction_id = " << it->first << ". Count is " << it->second.reply_count << " for key = " << it->second.key << " and value = " << it->second.value << endl;
    this->transaction_count.erase(it->first); // Now erase the transaction as it will not be used again
}

//pass the transaction count map iterator to this function to log the failure according to the message type
// These log messages are the messages of coordinator
void MP2Node::logCoordinatorFailure(map<int, transaction_details>::iterator it) {
    switch(this->transaction_count[it->first].rep_type){
        case CREATE : {log->logCreateFail(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
        case DELETE : {log->logDeleteFail(&getMemberNode()->addr, true, it->first, it->second.key); break;}
        case READ   : {log->logReadFail(&getMemberNode()->addr, true, it->first, it->second.key); break;}
        case UPDATE : {log->logUpdateFail(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
        default: {}
    }
    cout << "Failed transaction  for transaction_id = " << it->first << ". Count is " << it->second.reply_count << " for key = " << it->second.key << " and value = " << it->second.value << endl;
    this->transaction_count.erase(it->first); // Now erase the transaction
}

//Check the reply message received at the coordinator
void MP2Node::checkReplyMessages() {
    int curr_time = par->getcurrtime(); //get current time
    map<int, transaction_details>::iterator it = this->transaction_count.begin();
    cout << "For server = ";
    printAddress(&getMemberNode()->addr);
    cout << endl;
    pair<int, int> rep_count;
    int acks = 0, nacks = 0;

    while(it != this->transaction_count.end()) { // iterate over each pending transaction whose ack or nack not received.
        rep_count = this->countAcks(it->first); // count the number of acks and nacks
        acks = rep_count.first; nacks = rep_count.second;
        if ( (acks + nacks) <= 2){
            if (acks == 2){ // if ack == 2, the quorum in +ve replies so, log success at coordinator
                this->logCoordinatorSuccess(it);
            } else { // otherwise check if the reply by the server has timeout. If yes, then log failure
                if ((curr_time - it->second.timestamp) >= Reply_Timeout) {
                    this->logCoordinatorFailure(it);
                }
            }
        } else {
            if (acks < nacks){ // if acks < nacks when count = 3, then check for timeout and log failure if necessary.
                if ((curr_time - it->second.timestamp) >= Reply_Timeout) {
                    this->logCoordinatorFailure(it);
                }
            } else { // Otherwise log success at coordinator.
                    this->logCoordinatorSuccess(it);
            }
        }
        it++;
    }
}
/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
    cout << "In stablization " << endl;

    vector<Node> new_replicas, new_bosses;
    Node this_node = Node(getMemberNode()->addr); // Current Node

    for (int i = 0; i < ring.size(); i++) {

        if (isNodeSame(ring[i], this_node)) { // When we find the current node, assign the new replicas and predecessors(bosses) using same technique used in assignReplicationNodes()
            if (i == 0) {
                new_bosses.push_back(Node(*ring[ring.size() - 1].getAddress()));
                new_bosses.push_back(Node(*ring[ring.size() - 2].getAddress()));
            }else if (i == 1){
                new_bosses.push_back(Node(*ring[0].getAddress()));
                new_bosses.push_back(Node(*ring[ring.size() - 1].getAddress()));
            } else {
                new_bosses.push_back(Node(*ring[(i - 1) % ring.size()].getAddress()));
                new_bosses.push_back(Node(*ring[(i - 2) % ring.size()].getAddress()));
            }
            new_replicas.push_back(Node(*ring[(i + 1) % ring.size()].getAddress()));
            new_replicas.push_back(Node(*ring[(i + 2) % ring.size()].getAddress()));
        }
    }

    // CHeck new replicas first
    for (int i = 0; i < new_replicas.size(); i++) {
        if (isNodeSame(hasMyReplicas[i], new_replicas[i])) { // if the previous replica is again at same location then continue.
            continue;
        }
        else {
            // if this is the not the new replica for this key but its location has changed from Tertiary to Secondary because of failure of secondary.
            if (ifExistNode(hasMyReplicas, new_replicas[i]) != -1) {
                vector<pair<string, string>> keys = findMyKeys(PRIMARY);  //then find all the primary keys at this server and send update message to the server to change its replica type.
                Message *msg;
                for (int k = 0; k < keys.size(); k++) { // for each primary key at this node update the replica type of the node which has now become secondary from tertiary.
                    msg = new Message(-1, getMemberNode()->addr, UPDATE, keys[k].first, keys[k].second, static_cast<ReplicaType>(i + 1));
                    emulNet->ENsend(&getMemberNode()->addr, new_replicas[i].getAddress(), msg->toString());
                    free(msg);
                }
            } else {
                // if this is the new node that is now a replica, find all the primary keys at this node and send create messages to new node to insert those keys into their hashtable.
                vector<pair<string, string>> keys = findMyKeys(PRIMARY);
                Message *msg;
                for (int k = 0; k < keys.size(); k++) {
                    msg = new Message(-1, getMemberNode()->addr, CREATE, keys[k].first, keys[k].second, static_cast<ReplicaType>(i + 1));
                    emulNet->ENsend(&getMemberNode()->addr, new_replicas[i].getAddress(), msg->toString());
                    free(msg);
                }
            }
        }
    }

    // check for my bosses if they failed

    for (int i = 0; i < haveReplicasOf.size(); i++) {
        if (isNodeSame(haveReplicasOf[i], new_bosses[i])) {
            break; // if the boss is same and at same location, then break because boss will take care of its boss and will also correct this nodes replica type if necessary as is shown below.
        }
        else {
            if (ifExistNode(new_bosses, haveReplicasOf[i]) != -1) {
                break; // if there is some boss present which was also present earlier then don't do anything as it will take care of things and nodes in this system only leave, no join is there'
            } else {
                // else find keys at this node which are secondary or tertiary depending on how this node is located according to the new boss.
                vector<pair<string, string>> keys = findMyKeys(static_cast<ReplicaType>(i + 1));
                Entry *e; // Update keys at this local server. No need to send message since all the operations are local.
                for (int k = 0; k < keys.size(); k++){
                    e = new Entry(keys[k].second, par->getcurrtime(), PRIMARY);
                    ht->update(keys[k].first, e->convertToString());
                    free(e);
                }
                if (i == 0){ // If this was the secondary replica of the failed boss, then make your first replica secondary from tertiary by updating it.
                    // Also, add new tertiary replica by sending create messages for all the keys of the bosses which have now become primary at this server.
                    Message *msg;
                    for (int k = 0; k < keys.size(); k++) {
                        if (isNodeSame(new_replicas[0], hasMyReplicas[0])){
                            msg = new Message(-1, getMemberNode()->addr, UPDATE, keys[k].first, keys[k].second, SECONDARY);
                            emulNet->ENsend(&getMemberNode()->addr, new_replicas[0].getAddress(), msg->toString());
                        } else {
                            msg = new Message(-1, getMemberNode()->addr, CREATE, keys[k].first, keys[k].second, SECONDARY);
                            emulNet->ENsend(&getMemberNode()->addr, new_replicas[0].getAddress(), msg->toString());
                        }
                        free(msg);

                        msg = new Message(-1, getMemberNode()->addr, CREATE, keys[k].first, keys[k].second, TERTIARY);
                        emulNet->ENsend(&getMemberNode()->addr, new_replicas[1].getAddress(), msg->toString());
                        free(msg);
                    }
                } else if (i == 1){
                    // If this was the tertiary replica of the failed boss, then add both of your replicas
                    // by sending create messages for all the keys of the bosses which have now become primary at this server.
                    Message *msg;
                    for (int k = 0; k < keys.size(); k++) {
                        msg = new Message(-1, getMemberNode()->addr, CREATE, keys[k].first, keys[k].second, SECONDARY);
                        emulNet->ENsend(&getMemberNode()->addr, new_replicas[0].getAddress(), msg->toString());
                        free(msg);
                        msg = new Message(-1, getMemberNode()->addr, CREATE, keys[k].first, keys[k].second, TERTIARY);
                        emulNet->ENsend(&getMemberNode()->addr, new_replicas[1].getAddress(), msg->toString());
                        free(msg);
                    }
                }
            }
        }
    }

    // Now update the new replicas and bosses
    hasMyReplicas  = new_replicas;
    haveReplicasOf = new_bosses;

}

// Find Keys at this server that are residing here as of specific replica type.
vector<pair<string, string>> MP2Node::findMyKeys(ReplicaType rep_type) {
    map<string, string>::iterator iterator1 = ht->hashTable.begin();
    vector<pair<string, string>> keys;
    Entry *temp_e;
    while (iterator1 != ht->hashTable.end()){ // Loop over all the keys in the hashtable and find the key that belong to the passed replica type
        temp_e = new Entry(iterator1->second);
        if (temp_e->replica == rep_type){
            keys.push_back(pair<string, string>(iterator1->first, temp_e->value));
        }
        iterator1++;
        free(temp_e); // free the entry variable
    }
    return keys; // return vector of pair of keys and values.
}
/*
* Check if 2 nodes are same by checking their address are same or not using memcmp
* */
bool MP2Node::isNodeSame(Node n1, Node n2){
    if (memcmp(n1.getAddress()->addr, n2.getAddress()->addr, sizeof(Address)) == 0)
        return true;
    return false;
}

/*
* Check if some vector of nodes contain a given node.
* */
int MP2Node::ifExistNode(vector<Node> v, Node n1){
    vector<Node>::iterator iterator1 = v.begin();
    int i = 0;
    while(iterator1 != v.end()){ // iterate over each node in the vector
        if (isNodeSame(n1, *iterator1)) // if the nodes are same return the location otherwise return -1
            return i;
        iterator1++;
        i++;
    }
    return -1;
}

// Handle the create message coming at its correct destinations
void MP2Node::processCreate(Message incoming_msg) {
    // Call the server local function to insert key and value into its local hash table according to replica type.
    bool success_status = createKeyValue(incoming_msg.key, incoming_msg.value, incoming_msg.replica);
    int _trans_id = incoming_msg.transID; // get trans_id from the incoming message
    Address to_addr(incoming_msg.fromAddr); // get the coordinator address from where this message came

    if (_trans_id < 0) //if stablization came with trans_id = -1. This is special type of message that comes when ring repair work is going on. So, don't send its reply
        return;

    Message *msg = new Message(_trans_id, getMemberNode()->addr, REPLY, success_status);
    emulNet->ENsend(&getMemberNode()->addr, &to_addr, msg->toString()); // send reply message back to the coordinator

    // if the local create operation returned success then log server success otherwise log failure.
    if (success_status)
        log->logCreateSuccess(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, incoming_msg.value);
    else
        log->logCreateFail(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, incoming_msg.value);

    // free the message variable.
    free(msg);
}

// Handle the read message coming at its correct destinations
void MP2Node::processRead(Message incoming_msg) {
    // Call the server local function to read key and return value from its local hash table.
    string read_value = readKey(incoming_msg.key);
    cout << "Read from local " << read_value << " for key = " << incoming_msg.key << endl;
    int _trans_id = incoming_msg.transID; // get trans_id from the incoming message
    Address to_addr(incoming_msg.fromAddr); // get the coordinator address from where this message came

    if (_trans_id < 0)//if stablization came with trans_id = -1. This is special type of message that comes when ring repair work is going on. So, don't send its reply
        return;

    Message *msg = new Message(_trans_id, getMemberNode()->addr, read_value); // send ReadReply message back to the coordinator
    emulNet->ENsend(&getMemberNode()->addr, &to_addr, msg->toString());

    // if the local read operation returned non empty value then log server success otherwise log failure.
    if (!read_value.empty())
        log->logReadSuccess(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, read_value);
    else
        log->logReadFail(&getMemberNode()->addr, false, _trans_id, incoming_msg.key);

    // free the message variable
    free(msg);
}

// Handle the update message coming at its correct destinations
void MP2Node::processUpdate(Message incoming_msg) {
    // Call the server local function to update value corresponding to a key in its local hash table.
    bool success_status = updateKeyValue(incoming_msg.key, incoming_msg.value, incoming_msg.replica);
    int _trans_id = incoming_msg.transID; // get trans_id from the incoming message
    Address to_addr(incoming_msg.fromAddr); // get the coordinator address from where this message came

    if (_trans_id < 0)//if stablization came with trans_id = -1. This is special type of message that comes when ring repair work is going on. So, don't send its reply
        return;

    Message *msg = new Message(_trans_id, getMemberNode()->addr, REPLY, success_status); // send reply message back to the coordinator
    emulNet->ENsend(&getMemberNode()->addr, &to_addr, msg->toString());

    // if the local update operation returns success then log server success otherwise log failure.
    if (success_status)
        log->logUpdateSuccess(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, incoming_msg.value);
    else
        log->logUpdateFail(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, incoming_msg.value);

    // free message variable
    free(msg);
}

// Handle the delete message coming at its correct destinations
void MP2Node::processDelete(Message incoming_msg) {
    // Calls the server local function to delete key from its local hash table.
    bool success_status = deletekey(incoming_msg.key);
    int _trans_id = incoming_msg.transID; // get trans_id from the incoming message
    Address to_addr(incoming_msg.fromAddr); // get the coordinator address from where this message came

    if (_trans_id < 0)//if stablization came with trans_id = -1. This is special type of message that comes when ring repair work is going on. So, don't send its reply
        return;

    Message *msg = new Message(_trans_id, getMemberNode()->addr, REPLY, success_status); // send reply message back to the coordinator
    emulNet->ENsend(&getMemberNode()->addr, &to_addr, msg->toString());

    // if the local delete operation returns success then log server success otherwise log failure.
    if (success_status)
        log->logDeleteSuccess(&getMemberNode()->addr, false, _trans_id, incoming_msg.key);
    else
        log->logDeleteFail(&getMemberNode()->addr, false, _trans_id, incoming_msg.key);

    // free the message variable
    free(msg);
}

// Handle the ReadReply message coming at the coordinator
void MP2Node::processReadReply(Message incoming_msg){
    int _trans_id = incoming_msg.transID;// get trans_id from the incoming message
    string value = incoming_msg.value;// get the value returned by the server

    // if value not empty then save it as +ve ack along with value otherwise save it as -ve ack with empty null value for the specific transaction id of the operation.
    if (!value.empty()){
        Entry entry_value(value);
        this->transaction_count[_trans_id].value = entry_value.value;
        incTransactionReplyCount(_trans_id, 1, value);
    }
    else
        incTransactionReplyCount(_trans_id, -1, "");
}

// Handle the normal reply messages coming at the coordinator
void MP2Node::processNodesReply(Message incoming_msg) {
    Address from_addr(incoming_msg.fromAddr); // address from where the ack or nack came
    int _trans_id = incoming_msg.transID;// get trans_id from the incoming message
    bool success = incoming_msg.success;// success status returned by the server

    cout << "I got a reply from ";
    printAddress(&from_addr);
    cout << endl;

    // if status success save the ack as +ve ack otherwise -ve ack for the specific transaction id of the operation
    if (success)
        incTransactionReplyCount(_trans_id, 1, "");
    else
        incTransactionReplyCount(_trans_id, -1, "");
}
/*
*
* Check whether the ring is same as it was before getting the new members list.
*/
bool MP2Node::isRingSame(vector<Node> sortedMemList) {
    bool stablization_required = false;

    if (!ring.empty()) { // check if ring is not empty. Not make sense to run stablization on new incoming node
        if (ring.size() != sortedMemList.size()) //if the size is not same straightaway return true;
            stablization_required = true;
        else // otherwise check looping through ring to check if each member is same as before (required when there are joins)
            for (int i = 0; i < sortedMemList.size(); i++) {
                if (!isNodeSame(sortedMemList[i], ring[i])){
                    stablization_required = true;
                    break;
                }
            }
    }

    return stablization_required;
}

/**
*
* Assign successors and predecessors to this node i.e. change hasMyReplicas and haveReplicasOf
*
*/
void MP2Node::assignReplicationNodes() {
    Node this_node = Node(getMemberNode()->addr);
    if (hasMyReplicas.empty() || haveReplicasOf.empty()) {
        for (int i = 0; i < ring.size(); i++) { // Loop over ring size
            if (isNodeSame(ring[i], this_node)){ // When you find the current Node, then assign predecessors and successors.
                if (i == 0){ // if node is at 0 location, then this node contains the replicas of last and second last members in the ring
                    haveReplicasOf.push_back(Node(*ring[ring.size() - 1].getAddress()));
                    haveReplicasOf.push_back(Node(*ring[ring.size() - 2].getAddress()));
                } else if (i == 1){ // if node is at 1 location, then this node contains the replicas of 0th and last member in the ring
                    haveReplicasOf.push_back(Node(*ring[0].getAddress()));
                    haveReplicasOf.push_back(Node(*ring[ring.size() - 1].getAddress()));
                } else { // If no boundary conditions then, this node contains the replicas of the previous 2 nodes. Modulo just in case :)
                    haveReplicasOf.push_back(Node(*ring[(i - 1) % ring.size()].getAddress()));
                    haveReplicasOf.push_back(Node(*ring[(i - 2) % ring.size()].getAddress()));
                }
                // The replicas of this node will be at the next 2 nodes, hash so as to cover the case when next or next to next node is in the begining of the ring
                hasMyReplicas.push_back(Node(*ring[(i + 1) % ring.size()].getAddress()));
                hasMyReplicas.push_back(Node(*ring[(i + 2) % ring.size()].getAddress()));
            }
        }
    }

    // Just the debugging log to check if new and old rings are what they needs to be.
    cout << "Ring is as follows : " << endl;
    for (int i = 0; i < ring.size(); i++){
        printAddress(ring[i].getAddress());
        cout << " ";
    }
    cout << endl;
    cout << "Replicas of ";
    printAddress(&getMemberNode()->addr);
    cout << " are at " << endl;
    for (int i = 0; i < hasMyReplicas.size(); i++){
        printAddress(hasMyReplicas[i].getAddress());
        cout << endl;
    }
    cout << " and it has replicas of " << endl;
    for (int i = 0; i < haveReplicasOf.size(); i++){
        printAddress(haveReplicasOf[i].getAddress());
        cout << endl;
    }

}

// count the number of acks and nacks for a particular transaction
pair<int, int> MP2Node::countAcks(int _trans_id){
    map<int,transaction_details>::iterator it = this->transaction_count.find(_trans_id);
    int acks = 0, nacks = 0, reply_val;
    if (it != this->transaction_count.end()){
        vector<pair<int, string>>::iterator it2 = this->transaction_count[_trans_id].ackStore.begin();
        while(it2 != this->transaction_count[_trans_id].ackStore.end()){ // iterator over all the replies that came from the servers.
            reply_val = it2->first;
            if (reply_val == 1) // check the type of acknowledgement
                acks++;
            else
                nacks++;
            it2++;
        }
        cout << "Acks = "<<acks << " Nacks = " << nacks << endl;
        return pair<int, int>(acks, nacks); // return the number of acks and nacks as a pair if found otherwise 0,0.
    }
    else
        return pair<int, int>(0, 0);
}

// initialize the details of a particular transaction using its key, value, msg_type and timestamp
void MP2Node::initTransactionCount(int _trans_id, string key, string value, MessageType msg_type){
    map<int,transaction_details>::iterator it = this->transaction_count.find(_trans_id);

    if (it == this->transaction_count.end()){
        this->transaction_count[_trans_id].reply_count = 0; // set the reply count to 0
        this->transaction_count[_trans_id].key = key;
        this->transaction_count[_trans_id].value = value;
        this->transaction_count[_trans_id].rep_type = msg_type;
        this->transaction_count[_trans_id].timestamp = par->getcurrtime(); // set the time as current time
    }
    else
        return;
}
// Update the details of a given transaction according to its transaction id and increase the reply count and set the ack_type of the reply message along with the message.
void MP2Node::incTransactionReplyCount(int _trans_id, int ack_type, string incoming_message){

    map<int,transaction_details>::iterator it = this->transaction_count.find(_trans_id);

    if (it == this->transaction_count.end())
        return;
    else{
        this->transaction_count[_trans_id].ackStore.push_back(pair<int, string>(ack_type, incoming_message)); // save the message that came from the server.
        this->transaction_count[_trans_id].reply_count++; // increment the reply count
    }
}