/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once
#include <thread>

#include "interface/rdbc/net_channel.h"
#include "platform/common/queue/batch_queue.h"
#include "platform/common/queue/lock_free_queue.h"
#include "platform/networkstrate/async_replica_client.h"
#include "platform/proto/replica_info.pb.h"
#include "platform/proto/resdb.pb.h"
#include "platform/statistic/stats.h"

// Including the System.info class for node-shard identification
#include "platform/consensus/execution/system_info.h"

namespace resdb {

// ReplicaCommunicator is used for replicas to send messages
// between replicas.
class ReplicaCommunicator {
 public:
  ReplicaCommunicator(const std::vector<ReplicaInfo>& replicas,
                      SignatureVerifier* verifier = nullptr,
                      bool is_use_long_conn = false, int epoll_num = 1,
                      int tcp_batch = 1);
  virtual ~ReplicaCommunicator();

  // HeartBeat message is used to broadcast public keys.
  // It doesn't need the signature.
  virtual int SendHeartBeat(const Request& hb_info);

  virtual int SendMessage(const google::protobuf::Message& message);
  virtual int SendMessage(const google::protobuf::Message& message,
                          const ReplicaInfo& replica_info);

  virtual void BroadCast(const google::protobuf::Message& message);
  virtual void SendMessage(const google::protobuf::Message& message,
                           int64_t node_id);
  virtual int SendBatchMessage(
      const std::vector<std::unique_ptr<Request>>& messages,
      const ReplicaInfo& replica_info);
      
  //Note may need to adjust message sending functionality
  /**
  * Broadcasts a message to all nodes in a specified shard, it shard_id is not provided
  * Use current shard to commence broadcast
  * 
  * Parameters:
  * message: The message to be broadcast
  * system_info: The system info containing replica and shard info
  * shard_id: Optional shard ID (default is current shard_id)
  *
  * Returns:
  * Returns the number of successful sends via broadcast
  */

  int BroadCastToShard(const google::protobuf::Message& message,
                        SystemInfo* system_info,
                        int32_t shard_id = -1);

  /**
  * Sends a message to the coordinator replica of a specified shard.
  * If shard_id is not provided, sends to the current shard's coordinator
  *
  * Parameters:
  * message: The message to be broadcast
  * system_info: The system info containing replica and shard info
  * shard_id: Optional shard ID (default is current shard_id)
  *
  * Returns:
  * Returns code 1 for success and code 0 for failure
  */

  int SendToShardCoordinator(const google::protobuf::Message& message,
                            SystemInfo* system_info,
                            int32_t shard_id = -1);

  /**
  * Broadcasts a message to all shard leaders except the current shard's leader.
  *
  * Parameters:
  * message: The message to be broadcast
  * system_info: The system info containing replica and shard info
  *
  * Returns:
  * Returns the number of successful sends to shard coordinators
  */
  
  int BroadcastToOtherShardLeaders(const google::protobuf::Message& message,
                                  SystemInfo* system_info);

  /**
  * Gets the current shard ID for this node
  *
  * Parameters:
  * system_info: The system info containing replica and shard info
  * 
  * Returns:
  * The shard ID for the current node
  */                                

  uint32_t GetCurrentShardID(SystemInfo* system_info) const;

  /**
  * Helper function to get a collection of replica infos for a specific set of node IDs
  *
  * Parameters:
  * node_ids Vector of node IDs to get replica info for
  *
  * Returns:
  * Vector of ReplicaInfo for the requested nodes
  */

  std::vector<ReplicaInfo> GetReplicasForNodes(const std::vector<int64_t>& node_ids) const;
  
  void UpdateClientReplicas(const std::vector<ReplicaInfo>& replicas);
  std::vector<ReplicaInfo> GetClientReplicas();

 protected:
  virtual std::unique_ptr<NetChannel> GetClient(const std::string& ip,
                                                int port);
  virtual AsyncReplicaClient* GetClientFromPool(const std::string& ip,
                                                int port);

  void StartBroadcastInBackGround();
  int SendMessageInternal(const google::protobuf::Message& message,
                          const std::vector<ReplicaInfo>& replicas);
  int SendMessageFromPool(const google::protobuf::Message& message,
                          const std::vector<ReplicaInfo>& replicas);

  bool IsRunning() const;
  bool IsInPool(const ReplicaInfo& replica_info);

  void StartSingleInBackGround(const std::string& ip, int port);

  int SendSingleMessage(const google::protobuf::Message& message, 
      const ReplicaInfo& replica_info);

 private:
  std::vector<ReplicaInfo> replicas_;
  SignatureVerifier* verifier_;
  std::map<std::pair<std::string, int>, std::unique_ptr<AsyncReplicaClient>>
      client_pools_;
  std::thread broadcast_thread_;
  std::atomic<bool> is_running_;
  struct QueueItem {
    std::string data;
    std::vector<ReplicaInfo> dest_replicas;
  };
  BatchQueue<std::unique_ptr<QueueItem>> batch_queue_;
  bool is_use_long_conn_ = false;

  Stats* global_stats_;
  boost::asio::io_service io_service_;
  std::unique_ptr<boost::asio::io_service::work> worker_;
  std::vector<std::thread> worker_threads_;
  std::vector<ReplicaInfo> clients_;
  std::mutex mutex_;
  


  std::map<std::pair<std::string, int>, 
    std::unique_ptr<BatchQueue<std::unique_ptr<QueueItem>>>> single_bq_;
  std::vector<std::thread> single_thread_;
  int tcp_batch_;
  std::mutex smutex_;
};

}  // namespace resdb
