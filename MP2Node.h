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

#define RF 3 // Number of replicas
#define Reply_Timeout 4 // Reply timeout

typedef struct {
    int reply_count; // reply count
    MessageType rep_type; // message type
    int timestamp; // timestamp of the message
    string key; // key in the message
    string value; // value if in the message
    vector<pair<int, string>> ackStore; // data structure to store acks and nacks along with the values.
} transaction_details; // data structure to store details of a given transaction.

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

    // transaction_counter id for sending messages from this node to get reply with same id
    int transaction_id; // How can we use global transaction_id like g_transId because in real world that will require
	// too much communication between servers and may be concurrency issues if there is a leader.
	// So, I used a transaction_id per node which should also work in real world networks.


    // TransactionId with reply count
	//// Store transaction details defined in struct per each transaction
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

    //Assign hasReplicas and haveReplicas from the information about new ring.
    void assignReplicationNodes();

    // initialize the details of the given transaction to save it.
    void initTransactionCount(int _trans_id, string key, string value, MessageType msg_type);

	// increment the transaction reply count and save value according to ack or nack.
    void incTransactionReplyCount(int _trans_id, int ack_type, string incoming_message);

	//Count the number of acks and nacks for a given transaction.
    pair<int, int> countAcks(int _trans_id);

    //check if nodes are similar
    bool isNodeSame(Node n1, Node n2);
	//check if a given node exist in vector of nodes.
    int ifExistNode(vector<Node> v, Node n1);

    // find keys where I am replica of given type
    vector<pair<string, string>> findMyKeys(ReplicaType rep_type);


    // Handle for server create, read, delete, update, readreply and reply messages
    void processCreate(Message incoming_msg);
    void processRead(Message incoming_msg);
    void processDelete(Message incoming_msg);
    void processUpdate(Message incoming_msg);
    void processReadReply(Message incoming_msg);
    void processNodesReply(Message incoming_msg);

	// log functions at coordinator to log success or failure according to the type of message held by the iterator when message timeouts.
    void logCoordinatorSuccess(map<int, transaction_details>::iterator it);
    void logCoordinatorFailure(map<int, transaction_details>::iterator it);

    ~MP2Node();
};

#endif /* MP2NODE_H_ */
