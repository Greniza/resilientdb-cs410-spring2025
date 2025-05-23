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

#include "platform/networkstrate/replica_communicator.h"

#include <glog/logging.h>

#include <thread>

#include "platform/proto/broadcast.pb.h"

namespace resdb {

ReplicaCommunicator::ReplicaCommunicator(
    const std::vector<ReplicaInfo>& replicas, SignatureVerifier* verifier,
    bool is_use_long_conn, int epoll_num, int tcp_batch)
    : replicas_(replicas),
      verifier_(verifier),
      is_running_(false),
      batch_queue_("bc_batch", tcp_batch),
      is_use_long_conn_(is_use_long_conn),
      tcp_batch_(tcp_batch) {
  global_stats_ = Stats::GetGlobalStats();
  if (is_use_long_conn_) {
    worker_ = std::make_unique<boost::asio::io_service::work>(io_service_);
    for (int i = 0; i < epoll_num; ++i) {
      worker_threads_.push_back(std::thread([&]() { io_service_.run(); }));
    }
  }
  LOG(ERROR)<<" tcp batch:"<<tcp_batch;

  StartBroadcastInBackGround();

}

ReplicaCommunicator::~ReplicaCommunicator() {
  is_running_ = false;
  if (broadcast_thread_.joinable()) {
    broadcast_thread_.join();
  }
  if (is_use_long_conn_) {
    for (auto& cli : client_pools_) {
      cli.second.reset();
    }
    worker_.reset();
    worker_ = nullptr;
    io_service_.stop();
    for (auto& worker_th : worker_threads_) {
      if (worker_th.joinable()) {
        worker_th.join();
      }
    }
  }
}

bool ReplicaCommunicator::IsInPool(const ReplicaInfo& replica_info) {
  for (auto& replica : replicas_) {
    if (replica_info.ip() == replica.ip() &&
        replica_info.port() == replica.port()) {
      return true;
    }
  }
  return false;
}

bool ReplicaCommunicator::IsRunning() const { return is_running_; }

void ReplicaCommunicator::UpdateClientReplicas(
    const std::vector<ReplicaInfo>& replicas) {
  clients_ = replicas;
}

std::vector<ReplicaInfo> ReplicaCommunicator::GetClientReplicas() {
  return clients_;
}

int ReplicaCommunicator::SendHeartBeat(const Request& hb_info) {
  int ret = 0;
  for (const auto& replica : replicas_) {
    NetChannel client(replica.ip(), replica.port());
    if (client.SendRawMessage(hb_info) == 0) {
      ret++;
    }
  }
  return ret;
}

void ReplicaCommunicator::StartBroadcastInBackGround() {
  is_running_ = true;
  broadcast_thread_ = std::thread([&]() {
    while (IsRunning()) {
      std::vector<std::unique_ptr<QueueItem>> batch_req =
          batch_queue_.Pop(10000);
      if (batch_req.empty()) {
        continue;
      }
      BroadcastData broadcast_data;
      for (auto& queue_item : batch_req) {
        broadcast_data.add_data()->swap(queue_item->data);
      }

      global_stats_->SendBroadCastMsg(broadcast_data.data_size());
      int ret = SendMessageFromPool(broadcast_data, replicas_);
      if (ret < 0) {
        LOG(ERROR) << "broadcast request fail:";
      }
    }
  });
}

void ReplicaCommunicator::StartSingleInBackGround(const std::string& ip, int port) {
  single_bq_[std::make_pair(ip,port)] = std::make_unique<BatchQueue<std::unique_ptr<QueueItem>>>("s_batch", tcp_batch_);

  ReplicaInfo replica_info;
  for (const auto& replica : replicas_) {
    if (replica.ip() == ip && replica.port() == port) {
      replica_info = replica;
      break;
    }
  }

  if (replica_info.ip().empty()) {
    for (const auto& replica : GetClientReplicas()) {
      if (replica.ip() == ip && replica.port() == port) {
        replica_info = replica;
        break;
      }
    }
  }


  single_thread_.push_back(std::thread([&](BatchQueue<std::unique_ptr<QueueItem>> *bq, ReplicaInfo replica_info) {
    while (IsRunning()) {
      std::vector<std::unique_ptr<QueueItem>> batch_req =
          bq->Pop(50000);
      if (batch_req.empty()) {
        continue;
      }
      BroadcastData broadcast_data;
      for (auto& queue_item : batch_req) {
        broadcast_data.add_data()->swap(queue_item->data);
      }

      global_stats_->SendBroadCastMsg(broadcast_data.data_size());
      //LOG(ERROR)<<" send to ip:"<<replica_info.ip()<<" port:"<<replica_info.port()<<" bq size:"<<batch_req.size();
      int ret = SendMessageFromPool(broadcast_data, {replica_info});
      if (ret < 0) {
        LOG(ERROR) << "broadcast request fail:";
      }
      //LOG(ERROR)<<" send to ip:"<<replica_info.ip()<<" port:"<<replica_info.port()<<" bq size:"<<batch_req.size()<<" done";
    }
  }, single_bq_[std::make_pair(ip,port)].get(), replica_info));
}


int ReplicaCommunicator::SendSingleMessage(const google::protobuf::Message& message, 
const ReplicaInfo& replica_info) {

  std::string ip = replica_info.ip();
  int port = replica_info.port();

    //LOG(ERROR)<<" send msg ip:"<<ip<<" port:"<<port;
  global_stats_->BroadCastMsg();
  if (is_use_long_conn_) {
    auto item = std::make_unique<QueueItem>();
    item->data = NetChannel::GetRawMessageString(message, verifier_);
    std::lock_guard<std::mutex> lk(smutex_);
    if(single_bq_.find(std::make_pair(ip, port)) == single_bq_.end()){
      StartSingleInBackGround(ip, port);
    }
    assert(single_bq_[std::make_pair(ip, port)] != nullptr);
    single_bq_[std::make_pair(ip, port)]->Push(std::move(item));
    return 0;
  } else {
    return SendMessageInternal(message, replicas_);
  }
}

int ReplicaCommunicator::SendMessage(const google::protobuf::Message& message) {
  global_stats_->BroadCastMsg();
  if (is_use_long_conn_) {
    auto item = std::make_unique<QueueItem>();
    item->data = NetChannel::GetRawMessageString(message, verifier_);
    batch_queue_.Push(std::move(item));
    return 0;
  } else {
    return SendMessageInternal(message, replicas_);
  }
}

int ReplicaCommunicator::SendMessage(const google::protobuf::Message& message,
                                     const ReplicaInfo& replica_info) {
  return SendSingleMessage(message, replica_info);

  if (is_use_long_conn_) {
    std::string data = NetChannel::GetRawMessageString(message, verifier_);
    BroadcastData broadcast_data;
    broadcast_data.add_data()->swap(data);
    return SendMessageFromPool(broadcast_data, {replica_info});
  } else {
    return SendMessageInternal(message, {replica_info});
  }
}

int ReplicaCommunicator::SendBatchMessage(
    const std::vector<std::unique_ptr<Request>>& messages,
    const ReplicaInfo& replica_info) {
  if (is_use_long_conn_) {
    BroadcastData broadcast_data;
    for (const auto& message : messages) {
      std::string data = NetChannel::GetRawMessageString(*message, verifier_);
      broadcast_data.add_data()->swap(data);
    }
    return SendMessageFromPool(broadcast_data, {replica_info});
  } else {
    int ret = 0;
    for (const auto& message : messages) {
      ret += SendMessageInternal(*message, {replica_info});
    }
    return ret;
  }
}

int ReplicaCommunicator::SendMessageFromPool(
    const google::protobuf::Message& message,
    const std::vector<ReplicaInfo>& replicas) {
  int ret = 0;
  std::string data;
  message.SerializeToString(&data);
  global_stats_->SendBroadCastMsgPerRep();
  std::lock_guard<std::mutex> lk(mutex_);
  for (const auto& replica : replicas) {
    auto client = GetClientFromPool(replica.ip(), replica.port());
    if (client == nullptr) {
      continue;
    }
    //LOG(ERROR) << "send to:" << replica.ip();
    if (client->SendMessage(data) == 0) {
      ret++;
    } else {
      LOG(ERROR) << "send to:" << replica.ip() << " fail";
    }
    //LOG(ERROR) << "send to:" << replica.ip()<<" done";
  }
  return ret;
}

int ReplicaCommunicator::SendMessageInternal(
    const google::protobuf::Message& message,
    const std::vector<ReplicaInfo>& replicas) {
  int ret = 0;
  for (const auto& replica : replicas) {
    auto client = GetClient(replica.ip(), replica.port());
    if (client == nullptr) {
      continue;
    }
    if (verifier_ != nullptr) {
      client->SetSignatureVerifier(verifier_);
    }
    if (client->SendRawMessage(message) == 0) {
      ret++;
    }
  }
  return ret;
}

/*
std::vector<ReplicaInfo> ReplicaCommunicator::GetReplicasForNodes(
    const std::vector<uint32_t>& node_ids) const {
  std::vector<ReplicaInfo> target_replicas;

  //Check known replicas
  for (const auto& replica : replicas_) {
    if (std::find(node_ids.begin(), node_ids.end(), replica.id()) != node_ids.end()) {
      target_replicas.push_back(replica);
    }
  }

  // If not found in replicas, check client replicas too
  if (target_replicas.size() < node_ids.size()) {
    for (const auto& replica : GetClientReplicas()) {
      if (std::find(node_ids.begin(), node_ids.end(), replica.id()) != node_ids.end() &&
          std::find_if(target_replicas.begin(), target_replicas.end(),
                      [&](const ReplicaInfo& r) {return r.id() == replica.id(); }) == target_replicas.end()) {
        target_replicas.pushback(replica);
      }
    } 
  }
  return target_replicas;
}

int ReplicaCommunicator::BroadCastToShard(const google::protobuf::Message& message,
                                          SystemInfo* system_info,
                                          int32_t shard_id) {
  
  // Get all nodes in the target shard
  std::vector<uint32_t> shard_nodes = system_info->GetNodesInShard(shard_id);

  // If the target shard is empty log the error
  if (shard_nodes.empty()) {
    LOG(WARNING) << "Attempted to broadcast to an empty shard" << shard_id;
    return 0;
  }

  // Get ReplicaInfo objects for all nodes in the shard
  std::vector<ReplicaInfo> target_replicas = GetReplicasForNodes(shard_nodes);

  // Log an error if we can not find all the replicas within the shard
  if (target_replicas.size() < shard_nodes.size()) {
    LOG(WARNING) << "Could only find " << target_replicas.size()
                << " replicas out of " <<shard_nodes.size()
                << " nodes in shard " <<shard_id;
  }

  // Send the message to all replicas within the shard
  if (is_use_long_conn) {
    BroadcastData broadcast_data;
    std::string data = NetChannel::GetRawMessageString(message, verifier);
    broadcast_data.add_data()->swap(data);
    return SendMessageFromPool(broadcast_data, target_replicas);
  } else {
    return SendMessageInternal(message, target_replicas);
  }
}

int ReplicaCommunicator::SendToShardCoordinator(const google::protobuf::Message& message,
                                              SystemInfo* system_info,
                                              int32_t shard_id) {
  
  // Get the coordinator replica of the target shard
  uint32_t coordinator_id = system_info->GetPrimaryOfShard(shard_id);

  // Find the ReplicaInfo for the coordinator
  for (const auto& replica : replicas_) {
    if (replica.id() == coordinator_id) {
      // Found the coordinator -> sending message
      return SendMessage(message, replica);
    }
  }

  // Failed to find the coordinator
  LOG(ERROR) << "Coordinator for shard " << shard_id << " (node "<< coordinator_id <<") not found in replicas list";
  return 0;
}

int ReplicaCommunicator::BroadcastToShardParticipants(const google::protobuf::Message& message,
                                                    SystemInfo* system_info,
                                                    int32_t shard_id) {
  
  // Get all nodes in the target shard
  std::vector<uint32_t> shard_nodes = system_info->GetNodesInShard(shard_id);
  
  // Get the shard coordinator in the specified shard
  uint32_t coordinator_id = system_info->GetPrimaryOfShard(shard_id);

  // Storage for IDs
  std::vector<uint32_t> participant_ids;

  // Filter out the coordinator node
  for (const auto& node_id : shard_nodes) {
    if (node_id != coordinator_id) {
      participant_ids.push_back(node_id)
    };
  }
  // If the target shard is empty log the error
  if (participant_ids.empty()) {
    LOG(WARNING) << "Found no participant nodes in shard:" << shard_id;
    return 0;
  }

  // Get ReplicaInfo objects for all nodes in the shard
  std::vector<ReplicaInfo> target_replicas = GetReplicasForNodes(participant_ids);


  // Log an error if we can not find all the participant replicas
  if (target_replicas.size() < participant_ids.size()) {
    LOG(WARNING) << "Could only find " << target_replicas.size()
                << " replicas out of " <<participant_ids.size()
                << " nodes in shard " <<shard_id;
  }

  // Send the message to all replicas within the shard
  if (is_use_long_conn) {
    BroadcastData broadcast_data;
    std::string data = NetChannel::GetRawMessageString(message, verifier);
    broadcast_data.add_data()->swap(data);
    return SendMessageFromPool(broadcast_data, target_replicas);
  } else {
    return SendMessageInternal(message, target_replicas);
  }
}


int ReplicaCommunicator::BroadcastToAllShardLeaders(const google::protobuf::Message& message,
                                                    SystemInfo* system_info) {
  // Get total number of shards
  size_t shard_count = system_info->GetShardCount();

  // Collect all shard leader IDs
  std::vector<uint32_t> leader_ids;
  for (uint32_t shard_id = 0; shard_id < shard_count; ++shard_id) {
    leader_ids.push_back(system_info->GetPrimaryOfShard(shard_id));
  }

  // Get ReplicaInfo objects for all shard leaders
  std::vector<ReplicaInfo> leader_replicas = GetReplicasForNodes(leader_ids);

  // Log for the case where we cannot find all leader replicas
  if (leader_replicas.size() < leader_ids.size()) {
    LOG(WARNING) << "Could only find " << leader_replicas.size()
                << " replicas out of " << leader_ids.size()
                << " shard leaders";
  }

  // Send the message to all shard leaders
  if (is_use_long_conn_) {
    BroadcastData broadcast_data;
    std::string data = NetChannel::GetRawMessageString(message, verifier_);
    broadcast_data.add_data()->swap(data);
    return SendMessageFromPool(broadcast_data, leader_replicas);
  } else {
    return SendMessageInternal(message, leader_replicas);
  }
}
*/

AsyncReplicaClient* ReplicaCommunicator::GetClientFromPool(
    const std::string& ip, int port) {
  if (client_pools_.find(std::make_pair(ip, port)) == client_pools_.end()) {
    auto client = std::make_unique<AsyncReplicaClient>(
        &io_service_, ip, port + (is_use_long_conn_ ? 10000 : 0), true);
    client_pools_[std::make_pair(ip, port)] = std::move(client);
    //StartSingleInBackGround(ip, port);
  }
  return client_pools_[std::make_pair(ip, port)].get();
}

std::unique_ptr<NetChannel> ReplicaCommunicator::GetClient(
    const std::string& ip, int port) {
  return std::make_unique<NetChannel>(ip, port);
}

void ReplicaCommunicator::BroadCast(const google::protobuf::Message& message) {
  int ret = SendMessage(message);
  if (ret < 0) {
    LOG(ERROR) << "broadcast request fail:";
  }
}

void ReplicaCommunicator::SendMessage(const google::protobuf::Message& message,
                                      int64_t node_id) {
  ReplicaInfo target_replica;
  for (const auto& replica : replicas_) {
    if (replica.id() == node_id) {
      target_replica = replica;
      break;
    }
  }
  if (target_replica.ip().empty()) {
    for (const auto& replica : GetClientReplicas()) {
      if (replica.id() == node_id) {
        target_replica = replica;
        break;
      }
    }
  }

  if (target_replica.ip().empty()) {
    LOG(ERROR) << "no replica info";
    return;
  }

  int ret = SendMessage(message, target_replica);
  if (ret < 0) {
    LOG(ERROR) << "broadcast request fail:";
  }
}

}  // namespace resdb
