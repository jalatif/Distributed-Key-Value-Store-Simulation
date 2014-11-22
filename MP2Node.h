/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */

#define RF 3
#define Reply_Timeout 4

typedef struct {
    int reply_count;
    MessageType rep_type;
    int timestamp;
    string key;
    string value;
    vector<pair<int, string>> ackStore;
} transaction_details;

class MP2Node {
private:
	// Vector holding the next two neighbors in the ring who have my replicas
	vector<Node> hasMyReplicas;
	// Vector holding the previous two neighbors in the ring whose replicas I have
	vector<Node> haveReplicasOf;
	// Ring
	vector<Node> ring;
	// Hash Table
	HashTable * ht;
	// Member representing this member
	Member *memberNode;
	// Params object
	Params *par;
	// Object of EmulNet
	EmulNet * emulNet;
	// Object of Log
	Log * log;
    // transaction_counter
    int transaction_id;
    // TransactionId with reply count
    map<int, transaction_details> transaction_count;

public:
	MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);
	Member * getMemberNode() {
		return this->memberNode;
	}

	// ring functionalities
	void updateRing();
	vector<Node> getMembershipList();
	size_t hashFunction(string key);
	void findNeighbors();

	// client side CRUD APIs
	void clientCreate(string key, string value);
	void clientRead(string key);
	void clientUpdate(string key, string value);
	void clientDelete(string key);

	// receive messages from Emulnet
	bool recvLoop();
	static int enqueueWrapper(void *env, char *buff, int size);

	// handle messages from receiving queue
	void checkMessages();
    void checkReplyMessages();

	// coordinator dispatches messages to corresponding nodes
	void dispatchMessages(Message message);

	// find the addresses of nodes that are responsible for a key
	vector<Node> findNodes(string key);

	// server
	bool createKeyValue(string key, string value, ReplicaType replica);
	string readKey(string key);
	bool updateKeyValue(string key, string value, ReplicaType replica);
	bool deletekey(string key);

	// stabilization protocol - handle multiple failures
	void stabilizationProtocol();

    // print address
    void printAddress(Address *addr);

    // check if ring is same as before
    bool isRingSame(vector<Node> sortedMemList);

    //
    void assignReplicationNodes();

    //
    void initTransactionCount(int _trans_id, string key, string value, MessageType msg_type);
    void incTransactionReplyCount(int _trans_id, int ack_type, string incoming_message);

    // find prev and next neighbors in ring
    vector<Node> findMyBosses(string key);
    vector<Node> findMyReplicas(string key);

    //
    void processCreate(vector<string> message_by_parts);
    void processRead(vector<string> message_by_parts);
    void processNodesReply(vector<string> message_by_parts);
    void processDelete(vector<string> message_by_parts);
    void processReadReply(vector<string> message_by_parts);

    ~MP2Node();
};

#endif /* MP2NODE_H_ */
