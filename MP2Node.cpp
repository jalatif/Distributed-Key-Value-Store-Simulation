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
    this->transaction_id = 0;
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

    bool stablization_required = isRingSame(curMemList);

    if (ring.empty() || stablization_required)
        ring = curMemList;

    if (hasMyReplicas.empty() || haveReplicasOf.empty())
        assignReplicationNodes();

    cout << "Stablization required = " << stablization_required << endl;

    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
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

    vector<Node> destination_nodes = findNodes(key);
    for(int i = 0; i < destination_nodes.size(); i++){
        cout << "Key = " << key << " Replica = ";
        printAddress(destination_nodes[i].getAddress());
        cout << endl;
    }

    Message *msg;
    ReplicaType type;
    for (int i = 0; i < RF; i++){
        type = static_cast<ReplicaType>(i);
        msg = new Message(this->transaction_id, getMemberNode()->addr, CREATE, key, value, type);
        emulNet->ENsend(&getMemberNode()->addr, destination_nodes[i].getAddress(), msg->toString());
    }
    initTransactionCount(this->transaction_id++, key, value, CREATE);

    cout << "Message = " << msg->toString() << endl;
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

    vector<Node> destination_nodes = findNodes(key);

    Message *msg;
    for (int i = 0; i < RF; i++){
        msg = new Message(this->transaction_id, getMemberNode()->addr, READ, key);
        msg->delimiter = "::";
        emulNet->ENsend(&getMemberNode()->addr, destination_nodes[i].getAddress(), msg->toString());
    }
    initTransactionCount(this->transaction_id++, key, "", READ);

    cout << "RdMessage = " << msg->toString() << endl;

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


    vector<Node> destination_nodes = findNodes(key);

    Message *msg;
    for (int i = 0; i < RF; i++){
        msg = new Message(this->transaction_id, getMemberNode()->addr, UPDATE, key, value);
        msg->delimiter = "::";
        emulNet->ENsend(&getMemberNode()->addr, destination_nodes[i].getAddress(), msg->toString());
    }
    initTransactionCount(this->transaction_id++, key, value, UPDATE);

    cout << "UpMessage = " << msg->toString() << endl;
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

    vector<Node> destination_nodes = findNodes(key);

    Message *msg;
    for (int i = 0; i < RF; i++){
        msg = new Message(this->transaction_id, getMemberNode()->addr, DELETE, key);
        msg->delimiter = "::";
        emulNet->ENsend(&getMemberNode()->addr, destination_nodes[i].getAddress(), msg->toString());
    }
    initTransactionCount(this->transaction_id++, key, "", DELETE);

    cout << "DelMessage = " << msg->toString() << endl;
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
    cout << "Mereko na key aayi " << key << " aur value hai " << value << " aur replica bhi aaya = " << replica << endl;
    Entry e(value, par->getcurrtime(), replica);
    return ht->create(key, e.convertToString());
//    if (replica == PRIMARY && hasMyReplicas.empty()){
//        hasMyReplicas = findMyReplicas(key);
////        for (int i = 1; i < RF; i++){
////            hasMyReplicas.push_back(who_have_this_key[i]);
////        }
//
//    } else if (replica != PRIMARY && haveReplicasOf.empty()) {
//        vector<Node> myParentBoss = findMyBosses(key);
//        vector<Node> ourFamily = findNodes(key);
//        if (replica == SECONDARY){
//            haveReplicasOf.push_back(ourFamily[0]);
//            haveReplicasOf.push_back(myParentBoss[0]);
//        }
//        if (replica == TERTIARY){
//            haveReplicasOf.push_back(ourFamily[1]);
//            haveReplicasOf.push_back(ourFamily[0]);
//        }
////        vector<Node> who_have_this_key = findNodes(key);
////
////        if (std::find(haveReplicasOf.begin(), haveReplicasOf.end(), who_have_this_key[0]) == haveReplicasOf.end()){
////            haveReplicasOf.push_back(who_have_this_key[0]);
////            sort(haveReplicasOf.begin(), haveReplicasOf.end());
////        }
//    }
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

    return ht->read(key);
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
    Entry e(value, par->getcurrtime(), replica);
    return ht->update(key, e.convertToString());
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

    cout << "Del kar doon key = " << key << endl;
    bool success = ht->deleteKey(key);

    return success;
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


        string delimiter = "::";
        size_t delim_loc = 0;
        Message incoming_msg(message);

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

    checkReplyMessages();

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}


void MP2Node::logCoordinatorSuccess(map<int, transaction_details>::iterator it) {
    switch(this->transaction_count[it->first].rep_type){
        case CREATE : {log->logCreateSuccess(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
        case DELETE : {log->logDeleteSuccess(&getMemberNode()->addr, true, it->first, it->second.key); break;}
        case READ   : {
            if (!it->second.value.empty())
                log->logReadSuccess(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value);
            else
                log->logReadFail(&getMemberNode()->addr, true, it->first, it->second.key + "coord");
            break;
        }
        case UPDATE : {log->logUpdateSuccess(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
        default: {}
    }
    cout << "Success transaction  for transaction_id = " << it->first << ". Count is " << it->second.reply_count << " for key = " << it->second.key << " and value = " << it->second.value << endl;
    this->transaction_count.erase(it->first);
}

void MP2Node::logCoordinatorFailure(map<int, transaction_details>::iterator it) {
    switch(this->transaction_count[it->first].rep_type){
        case CREATE : {log->logCreateFail(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
        case DELETE : {log->logDeleteFail(&getMemberNode()->addr, true, it->first, it->second.key); break;}
        case READ   : {log->logReadFail(&getMemberNode()->addr, true, it->first, it->second.key); break;}
        case UPDATE : {log->logUpdateFail(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
        default: {}
    }
    cout << "Failed transaction  for transaction_id = " << it->first << ". Count is " << it->second.reply_count << " for key = " << it->second.key << " and value = " << it->second.value << endl;
    this->transaction_count.erase(it->first);
}

void MP2Node::checkReplyMessages() {
    int curr_time = par->getcurrtime();
    map<int, transaction_details>::iterator it = this->transaction_count.begin();
    cout << "For server = ";
    printAddress(&getMemberNode()->addr);
    cout << endl;
    pair<int, int> rep_count;
    int acks = 0, nacks = 0;

    while(it != this->transaction_count.end()) {
        rep_count = this->countAcks(it->first);
        acks = rep_count.first; nacks = rep_count.second;
        if ( (acks + nacks) <= 2){
            if (acks == 2){
                this->logCoordinatorSuccess(it);
            } else {
                if ((curr_time - it->second.timestamp) >= Reply_Timeout) {
                    this->logCoordinatorFailure(it);
                }
            }
        } else {
            if (acks < nacks){
                if ((curr_time - it->second.timestamp) >= Reply_Timeout) {
                    this->logCoordinatorFailure(it);
                }
            } else {
                    this->logCoordinatorSuccess(it);
            }
        }
//        if (it->second.reply_count < 2) {
//            if ((curr_time - it->second.timestamp) >= Reply_Timeout) {
//                switch(this->transaction_count[it->first].rep_type){
//                    case CREATE : {log->logCreateFail(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
//                    case READ   : {log->logReadFail(&getMemberNode()->addr, true, it->first, it->second.key); break;}
//                    case DELETE : {log->logDeleteFail(&getMemberNode()->addr, true, it->first, it->second.key); break;}
//                    default: {}
//                }
//                cout << "Failed transaction  for transaction_id = " << it->first << ". Count is " << it->second.reply_count << " for key = " << it->second.key << " and value = " << it->second.value << endl;
//                this->transaction_count.erase(it->first);
//            }
//        } else {
//            switch(this->transaction_count[it->first].rep_type){
//                case CREATE : {log->logCreateSuccess(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value); break;}
//                case READ   : {log->logReadSuccess(&getMemberNode()->addr, true, it->first, it->second.key, it->second.value + "coord("); break;}
//                case DELETE : {log->logDeleteSuccess(&getMemberNode()->addr, true, it->first, it->second.key); break;}
//                default: {}
//            }
//            cout << "Success transaction  for transaction_id = " << it->first << ". Count is " << it->second.reply_count << " for key = " << it->second.key << " and value = " << it->second.value << endl;
//            this->transaction_count.erase(it->first);
//        }
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
}

void MP2Node::processCreate(Message incoming_msg) {
    bool success_status = createKeyValue(incoming_msg.key, incoming_msg.value, incoming_msg.replica);
    int _trans_id = incoming_msg.transID;
    Message *msg;
    msg = new Message(_trans_id, getMemberNode()->addr, REPLY, success_status);
    msg->delimiter = "::";
    Address to_addr(incoming_msg.fromAddr);
    emulNet->ENsend(&getMemberNode()->addr, &to_addr, msg->toString());
    if (success_status)
        log->logCreateSuccess(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, incoming_msg.value);
    else
        log->logCreateFail(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, incoming_msg.value);
}


void MP2Node::processRead(Message incoming_msg) {
    string read_value = readKey(incoming_msg.key);
    cout << "Read from local " << read_value << " for key = " << incoming_msg.key << endl;
    int _trans_id = incoming_msg.transID;
    Message *msg;
    msg = new Message(_trans_id, getMemberNode()->addr, read_value);
    msg->delimiter = "::";
    Address to_addr(incoming_msg.fromAddr);
    emulNet->ENsend(&getMemberNode()->addr, &to_addr, msg->toString());
    if (!read_value.empty())
        log->logReadSuccess(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, read_value);
    else
        log->logReadFail(&getMemberNode()->addr, false, _trans_id, incoming_msg.key);
}

void MP2Node::processUpdate(Message incoming_msg) {
    bool success_status = updateKeyValue(incoming_msg.key, incoming_msg.value, incoming_msg.replica);
    int _trans_id = incoming_msg.transID;
    cout << "I am in update " << endl;

    Message *msg;
    msg = new Message(_trans_id, getMemberNode()->addr, REPLY, success_status);
    msg->delimiter = "::";
    Address to_addr(incoming_msg.fromAddr);
    emulNet->ENsend(&getMemberNode()->addr, &to_addr, msg->toString() + msg->delimiter);

    if (success_status)
        log->logUpdateSuccess(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, incoming_msg.value);
    else{
        log->logUpdateFail(&getMemberNode()->addr, false, _trans_id, incoming_msg.key, incoming_msg.value);
    }
}

void MP2Node::processDelete(Message incoming_msg) {
    bool success_status = deletekey(incoming_msg.key);
    int _trans_id = incoming_msg.transID;
    cout << "I am in del " << endl;

    Message *msg;
    msg = new Message(_trans_id, getMemberNode()->addr, REPLY, success_status);
    msg->delimiter = "::";
    Address to_addr(incoming_msg.fromAddr);
    emulNet->ENsend(&getMemberNode()->addr, &to_addr, msg->toString() + msg->delimiter);

    if (success_status)
        log->logDeleteSuccess(&getMemberNode()->addr, false, _trans_id, incoming_msg.key);
    else{
        log->logDeleteFail(&getMemberNode()->addr, false, _trans_id, incoming_msg.key);
    }
}

void MP2Node::processReadReply(Message incoming_msg){
    int _trans_id = incoming_msg.transID;
    string value = incoming_msg.value;
    cout << "Value read from the DHT" << value << endl;
    if (!value.empty()){
        Entry entry_value(value);
        this->transaction_count[_trans_id].value = entry_value.value;
        incTransactionReplyCount(_trans_id, 1, value);
    }
    else
        incTransactionReplyCount(_trans_id, -1, "");
}

void MP2Node::processNodesReply(Message incoming_msg) {
    Address from_addr(incoming_msg.fromAddr);
    int _trans_id = incoming_msg.transID;
    bool success = incoming_msg.success;

    cout << "I got a reply from ";
    printAddress(&from_addr);
    cout << endl;

    if (success)
        incTransactionReplyCount(_trans_id, 1, "");
    else
        incTransactionReplyCount(_trans_id, -1, "");//transaction_count[_trans_id].reply_count = -1 * (RF + 1);
}

bool MP2Node::isRingSame(vector<Node> sortedMemList) {
    bool stablization_required = false;

    if (!ring.empty()) {
        if (ring.size() != sortedMemList.size())
            stablization_required = true;
        else
            for (int i = 0; i < sortedMemList.size(); i++) {
                //std::string str1(sortedMemList.at(i).getAddress()->addr);
                //std::string str2(ring.at(i).getAddress()->addr);
                if (memcmp(sortedMemList[i].getAddress(), ring[i].getAddress(), sizeof(Address)) != 0){
                    //if (str1.compare(str2) == 0){
                    stablization_required = true;
                    break;
                }
            }
    }

    return stablization_required;
}

void MP2Node::assignReplicationNodes() {
    if (hasMyReplicas.empty() || haveReplicasOf.empty()) {
        for (int i = 0; i < ring.size(); i++) {
            if (memcmp(ring[i].getAddress()->addr, &getMemberNode()->addr, sizeof(Address)) == 0) {

                haveReplicasOf.push_back(Node(*ring[(i - 1) % ring.size()].getAddress()));
                haveReplicasOf.push_back(Node(*ring[(i - 2) % ring.size()].getAddress()));

                hasMyReplicas.push_back(Node(*ring[(i + 1) % ring.size()].getAddress()));
                hasMyReplicas.push_back(Node(*ring[(i + 2) % ring.size()].getAddress()));
            }
        }
    }

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
//
//struct by_timestamp {
//    bool operator()(entry const &a,  const &b) const {
//        return a.age < b.age;
//    }
//};

pair<int, int> MP2Node::countAcks(int _trans_id){
    map<int,transaction_details>::iterator it = this->transaction_count.find(_trans_id);
    int acks = 0, nacks = 0, reply_val;
    if (it != this->transaction_count.end()){
        vector<pair<int, string>>::iterator it2 = this->transaction_count[_trans_id].ackStore.begin();
        while(it2 != this->transaction_count[_trans_id].ackStore.end()){
            reply_val = it2->first;
            if (reply_val == 1)
                acks++;
            else
                nacks++;
            it2++;
        }
        cout << "Acks = "<<acks << " Nacks = " << nacks << endl;
        return pair<int, int>(acks, nacks);
    }
    else
        return pair<int, int>(0, 0);
}

void MP2Node::initTransactionCount(int _trans_id, string key, string value, MessageType msg_type){
    map<int,transaction_details>::iterator it = this->transaction_count.find(_trans_id);

    if (it == this->transaction_count.end()){
        this->transaction_count[_trans_id].reply_count = 0;
        this->transaction_count[_trans_id].key = key;
        this->transaction_count[_trans_id].value = value;
        this->transaction_count[_trans_id].rep_type = msg_type;
        this->transaction_count[_trans_id].timestamp = par->getcurrtime();
    }
    else
        return;
}

void MP2Node::incTransactionReplyCount(int _trans_id, int ack_type, string incoming_message){

    map<int,transaction_details>::iterator it = this->transaction_count.find(_trans_id);

    if (it == this->transaction_count.end())
        return;
    else{
        this->transaction_count[_trans_id].ackStore.push_back(pair<int, string>(ack_type, incoming_message));
        this->transaction_count[_trans_id].reply_count++;
    }
}

vector<Node> MP2Node::findMyBosses(string key){
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
            addr_vec.emplace_back(ring.at(ring.size() - 1));
            addr_vec.emplace_back(ring.at(ring.size() - 2));
        }
        else if (pos > ring.at(0).getHashCode() && pos <= ring.at(1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(ring.size() - 1));
        }
        else {
            // go through the ring until pos <= node
            for (int i=2; i<ring.size(); i++){
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(ring.at((i-1)%ring.size()));
                    addr_vec.emplace_back(ring.at((i-2)%ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

vector<Node> MP2Node::findMyReplicas(string key){
    size_t pos = hashFunction(key);
    vector<Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        }
        else {
            // go through the ring until pos <= node
            for (int i=1; i<ring.size(); i++){
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(ring.at((i+1)%ring.size()));
                    addr_vec.emplace_back(ring.at((i+2)%ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}