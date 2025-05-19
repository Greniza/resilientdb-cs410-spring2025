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

#include "platform/consensus/execution/system_info.h"

#include <glog/logging.h>

namespace resdb {

SystemInfo::SystemInfo() : primary_id_(1), view_(1) {}

SystemInfo::SystemInfo(const ResDBConfig& config)
    : primary_id_(config.GetReplicaInfos()[0].id()), view_(1) {
  SetReplicas(config.GetReplicaInfos());
  LOG(ERROR) << "get primary id:" << primary_id_;
}

uint32_t SystemInfo::GetPrimaryId() const { return primary_id_; }

void SystemInfo::SetPrimary(uint32_t id) { primary_id_ = id; }

uint64_t SystemInfo::GetCurrentView() const { return view_; }

void SystemInfo::SetCurrentView(uint64_t view_id) { view_ = view_id; }

std::vector<ReplicaInfo> SystemInfo::GetReplicas() const { return replicas_; }

void SystemInfo::SetReplicas(const std::vector<ReplicaInfo>& replicas) {
  replicas_ = replicas;
}

void SystemInfo::AddReplica(const ReplicaInfo& replica) {
  if (replica.id() == 0 || replica.ip().empty() || replica.port() == 0) {
    return;
  }
  for (const auto& cur_replica : replicas_) {
    if (cur_replica.id() == replica.id()) {
      LOG(ERROR) << " replica exist:" << replica.id();
      return;
    }
  }
  LOG(ERROR) << "add new replica:" << replica.DebugString();
  AddReplicaToShard(replica);
}

void SystemInfo::ProcessRequest(const SystemInfoRequest& request) {
  switch (request.type()) {
    case SystemInfoRequest::ADD_REPLICA: {
      NewReplicaRequest info;
      if (info.ParseFromString(request.request())) {
        AddReplica(info.replica_info());
      }
    } break;
    default:
      break;
  }
}

//Implemented For Assignment 3 

//Returns # of Shards
size_t SystemInfo::GetShardCount() const {
  return shard_count_;
}

//Returns # of Nodes In Shard with given ID
//Returns 0 if shard doesn't exist
size_t SystemInfo::GetShardSize(uint32_t shard_id) const {
    auto it = shard_to_nodes_.find(shard_id);    
    return it != shard_to_nodes_.end() ? it->second.size() : 0;                        
                                                
}

//Returns a vector of node ID's apart of shard
//If Does Not Exist, returns empty
std::vector<uint32_t> SystemInfo::GetNodesInShard(uint32_t shard_id) const {
   auto it = shard_to_nodes_.find(shard_id);   
   return it != shard_to_nodes_.end() ? it->second : std::vector<uint32_t>{}; 
}

//Returns Specific ID of given node
//Reutns invalid id if not assigned
uint32_t SystemInfo::GetShardOfNode(uint32_t node_id) const {
  auto it = node_to_shard_.find(node_id);
  return it != node_to_shard_.end() ? it->second : UINT32_MAX;
}

//Returns the NodeID that is primary for a given shard ID
//Returns invalid if there is no primary
uint32_t SystemInfo::GetPrimaryOfShard(uint32_t shard_id) const {
  auto it = shard_primaries_.find(shard_id);
  return it != shard_primaries_.end() ? it->second : UINT_MAX;

}

//Sets # of Shards & clears shard mapping
void SystemInfo::SetShardCount(size_t count) {

  shard_count_ = count;
  node_to_shard_.clear();
  shard_to_nodes_.clear();
  shard_primaries_.clear();
}




 // overrides AddReplica()
 // Adds a replica to the least populated shard
 // Tracks shard membership & designates first node in shard 
void SystemInfo::AddReplicaToShard(const ReplicaInfo& replica)  {
 //Check for initalization
  if(shard_count_ == 0) {
    LOG(ERROR) << "Set the shard count";
    return;
  }

  //Find shard with smallest # of nodes
  size_t target = 0;
  size_t min_size = SIZE_MAX;

  for(unsigned int i = 0; i < shard_count_; ++i){
    size_t sz = shard_to_nodes_[i].size();
    if (sz < min_size){
      target = i;
      min_size = sz;
    }
  }
  //Add replica
  replicas_.push_back(replica);

  //Record which shard the node belongs to 
  node_to_shard_[replica.id()] = target;

  //Add node to list
  shard_to_nodes_[target].push_back(replica.id());

  //Check heirarchy, and designates as primary if it is first
  if(shard_primaries_.find(target) == shard_primaries_.end()){
    shard_primaries_[target] = replica.id();
  }
  //Log for debug
  LOG(INFO) << "Node: " << replica.id() << "Shard: " << target;

}

} // namespace resdb
 