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

#include "platform/consensus/ordering/pbft/commitment.h"

#include <glog/logging.h>
#include <unistd.h>

#include "common/utils/utils.h"
#include "platform/consensus/ordering/pbft/transaction_utils.h"

namespace resdb {

Commitment::Commitment(const ResDBConfig& config,
                       MessageManager* message_manager,
                       ReplicaCommunicator* replica_communicator,
                       SignatureVerifier* verifier)
    : config_(config),
      message_manager_(message_manager),
      stop_(false),
      replica_communicator_(replica_communicator),
      verifier_(verifier) {
  executed_thread_ = std::thread(&Commitment::PostProcessExecutedMsg, this);
  global_stats_ = Stats::GetGlobalStats();
  duplicate_manager_ = std::make_unique<DuplicateManager>(config);
  message_manager_->SetDuplicateManager(duplicate_manager_.get());

  global_stats_->SetProps(
      config_.GetSelfInfo().id(), config_.GetSelfInfo().ip(),
      config_.GetSelfInfo().port(), config_.GetConfigData().enable_resview(),
      config_.GetConfigData().enable_faulty_switch());
  global_stats_->SetPrimaryId(message_manager_->GetCurrentPrimary());
}

Commitment::~Commitment() {
  stop_ = true;
  if (executed_thread_.joinable()) {
    executed_thread_.join();
  }
}

void Commitment::SetPreVerifyFunc(
    std::function<bool(const Request& request)> func) {
  pre_verify_func_ = func;
}

void Commitment::SetNeedCommitQC(bool need_qc) { need_qc_ = need_qc; }

// Handle the user request and send a pre-prepare message to others.
// Project 3: Make it so that the shard that recieves the request becomes the primary shard for that txn.
int Commitment::ProcessNewRequest(std::unique_ptr<Context> context, std::unique_ptr<Request> user_request) {
  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }

  if (uint64_t seq =
          duplicate_manager_->CheckIfExecuted(user_request->hash())) {
    LOG(ERROR) << "This request is already executed with seq: " << seq;
    user_request->set_seq(seq);
    message_manager_->SendResponse(std::move(user_request));
    return -2;
  }

  if (config_.GetSelfInfo().id() != message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id())) {
    // LOG(ERROR) << "current node is not primary. primary:"
    //            << message_manager_->GetCurrentPrimary()
    //            << " seq:" << user_request->seq()
    //            << " hash:" << user_request->hash();
    LOG(INFO) << "NOT SHARD COORDINATOR, shard_coordinator is "
              << message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id());
    replica_communicator_->SendMessage(*user_request,
                                       message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id()));
    {
      std::lock_guard<std::mutex> lk(rc_mutex_);
      request_complained_.push(
          std::make_pair(std::move(context), std::move(user_request)));
    }

    return -3;
  }

  /*
  if(SignatureVerifier::CalculateHash(user_request->data()) !=
  user_request->hash()){ LOG(ERROR) << "the hash and data of the user request
  don't match, reject"; return -2;
  }
  */

  // check signatures
  bool valid = verifier_->VerifyMessage(user_request->data(),
                                        user_request->data_signature());
  if (!valid) {
    LOG(ERROR) << "request is not valid:"
               << user_request->data_signature().DebugString();
    LOG(ERROR) << " msg:" << user_request->data().size();
    return -2;
  }

  if (pre_verify_func_ && !pre_verify_func_(*user_request)) {
    LOG(ERROR) << " check by the user func fail";
    return -2;
  }

  global_stats_->IncClientRequest();
  if (duplicate_manager_->CheckAndAddProposed(user_request->hash())) {
    return -2;
  }
  auto seq = message_manager_->AssignNextSeq();

  // Artificially make the primary stop proposing new trasactions.

  if (!seq.ok()) {
    duplicate_manager_->EraseProposed(user_request->hash());
    global_stats_->SeqFail();
    Request request;
    request.set_type(Request::TYPE_RESPONSE);
    request.set_sender_id(config_.GetSelfInfo().id());
    request.set_proxy_id(user_request->proxy_id());
    request.set_ret(-2);
    request.set_hash(user_request->hash());

    replica_communicator_->SendMessage(request, request.proxy_id());
    return -2;
  }

  global_stats_->RecordStateTime("request");

  user_request->set_type(Request::TYPE_PRE_PREPARE);
  user_request->set_current_view(message_manager_->GetCurrentView());
  user_request->set_seq(*seq);
  user_request->set_sender_id(config_.GetSelfInfo().id());
  user_request->set_primary_id(config_.GetSelfInfo().id());

  // Project 3: Broadcast to shard coordinators instead of all nodes
  for (int i = 0; i<message_manager_->GetShardCount(); i++) {
        replica_communicator_->SendMessage(*user_request, message_manager_->GetPrimaryOfShard(i));
    }
  // replica_communicator_->BroadcastToAllShardLeaders(*user_request, message_manager_);

  return 0;
}

// Receive the pre-prepare message from the primary.
int Commitment::ProcessProposeMsg(std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // If a non-coordinator recieves a message from outside the shard, retransmit to shard coord and end.
  if (! (message_manager_->NodesInSameShard(request->sender_id(), config_.GetSelfInfo().id()))
      && (config_.GetSelfInfo().id() != message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id()))) {
    
    // Retransmit to shard coord
    uint32_t shard = message_manager_->GetShardOfNode(config_.GetSelfInfo().id());
    replica_communicator_->SendMessage(*request, message_manager_->GetPrimaryOfShard(shard));

    LOG(INFO) << "Subnode recieved message meant for shard coordinator in, "
              << message_manager_->GetPrimaryOfShard(shard);
    return -3;
  }
  if (global_stats_->IsFaulty() || context == nullptr ||
      context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }
  if (request->is_recovery()) {
    if (message_manager_->GetNextSeq() == 0 ||
        request->seq() == message_manager_->GetNextSeq()) {
      message_manager_->SetNextSeq(request->seq() + 1);
    } else {
      LOG(ERROR) << " recovery request not valid:"
                 << " current seq:" << message_manager_->GetNextSeq()
                 << " data seq:" << request->seq();
      return 0;
    }
    return message_manager_->AddConsensusMsg(context->signature,
                                             std::move(request));
  }


  // Propose may come from either shard coord or primary
  if ((request->sender_id() != message_manager_->GetCurrentPrimary()) &&
      (request->sender_id() != message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id()))) {
    LOG(ERROR) << "the request is not from primary/shard coordinator. sender:"
               << request->sender_id() << " seq:" << request->seq();
    return -2;
  }

  /*
    if(request->hash() != "null" + std::to_string(request->seq())
        && SignatureVerifier::CalculateHash(request->data()) != request->hash())
    { LOG(ERROR) << "the hash and data of the request don't match, reject";
      return -2;
    }
    */

  if (request->sender_id() != config_.GetSelfInfo().id()) {
    if (pre_verify_func_ && !pre_verify_func_(*request)) {
      LOG(ERROR) << " check by the user func fail";
      return -2;
    }
    // global_stats_->GetTransactionDetails(std::move(request));
    BatchUserRequest batch_request;
    batch_request.ParseFromString(request->data());
    batch_request.clear_createtime();
    std::string data;
    batch_request.SerializeToString(&data);
    // check signatures
    bool valid =
        verifier_->VerifyMessage(request->data(), request->data_signature());
    if (!valid) {
      LOG(ERROR) << "request is not valid:"
                 << request->data_signature().DebugString();
      LOG(ERROR) << " msg:" << request->data().size();
      return -2;
    }
    if (duplicate_manager_->CheckAndAddProposed(request->hash())) {
      LOG(INFO) << "The request is already proposed, reject";
      return -2;
    }
  }

  global_stats_->IncPropose();
  global_stats_->RecordStateTime("pre-prepare");
  std::unique_ptr<Request> prepare_request = resdb::NewRequest(
      Request::TYPE_PREPARE, *request, config_.GetSelfInfo().id());
  prepare_request->clear_data();

  // Add request to message_manager.
  // Changed for project 3
  CollectorResultCode ret = message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    
    if (message_manager_->GetTransactionState(request->seq()) == TransactionStatue::READY_PREPARE) {
      // (PHASE 1)
      replica_communicator_->SendMessage(*prepare_request, config_.GetSelfInfo().id());
    
      if (request->sender_id() != message_manager_->GetCurrentPrimary()) {
        replica_communicator_->SendMessage(*prepare_request,  message_manager_->GetCurrentPrimary());
      }
    }
    else {
      // (PHASE 3.5)
      // Broadcast prepare to entire shard
      for (int i = 0; i<message_manager_->GetShardOfNode(message_manager_->GetShardSize(message_manager_->GetShardOfNode(config_.GetSelfInfo().id()))); i++)
        replica_communicator_->SendMessage(*prepare_request, message_manager_->GetNodesInShard(message_manager_->GetShardOfNode(config_.GetSelfInfo().id()))[i]);
    }
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// If receive 2f+1 prepare message, broadcast a commit message.
int Commitment::ProcessPrepareMsg(std::unique_ptr<Context> context,std::unique_ptr<Request> request) {
  // If a non-coordinator recieves a message from outside the shard, retransmit to shard coord and end.
  if (! (message_manager_->NodesInSameShard(request->sender_id(), config_.GetSelfInfo().id()))
      && (config_.GetSelfInfo().id() != message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id()))) {
    
    // Retransmit to shard coord
    uint32_t shard = message_manager_->GetShardOfNode(config_.GetSelfInfo().id());
    replica_communicator_->SendMessage(*request, message_manager_->GetPrimaryOfShard(shard));

    LOG(INFO) << "Subnode recieved message meant for coordinator, "
              << message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id());
    return -3;
  }
  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject";
    return -2;
  }
  if (request->is_recovery()) {
    return message_manager_->AddConsensusMsg(context->signature,
                                             std::move(request));
  }
  //global_stats_->IncPrepare();
  std::unique_ptr<Request> commit_request = resdb::NewRequest(
      Request::TYPE_COMMIT, *request, config_.GetSelfInfo().id());
  commit_request->mutable_data_signature()->Clear();
  // Add request to message_manager.
  // If it has received enough same requests(2f+1), broadcast the commit
  // message.
  uint64_t seq_ = request->seq();
  CollectorResultCode ret = message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    if (message_manager_->GetHighestPreparedSeq() < seq_) {
      message_manager_->SetHighestPreparedSeq(seq_);
    }
    // If need qc, sign the data
    if (need_qc_ && verifier_) {
      auto signature_or = verifier_->SignMessage(commit_request->hash());
      if (!signature_or.ok()) {
        LOG(ERROR) << "Sign message fail";
        return -2;
      }
      *commit_request->mutable_data_signature() = *signature_or;
      // LOG(ERROR) << "sign hash"
      //           << commit_request->data_signature().DebugString();
    }

    // Project 3
    // If we're in the top-level 2pc, handle it
    if (message_manager_->GetTransactionState(seq_) == TransactionStatue::READY_COMMIT) {
      // (PHASE 2)
      global_stats_->RecordStateTime("prepare");
      if (config_.GetSelfInfo().id() == message_manager_->GetCurrentPrimary()) {
        replica_communicator_->BroadCast(*commit_request);
        // 2PC MOD
      }
    }
    else {
      // We're on the local phase, so we broadcast the commit message locally
      for (int i = 0; i<message_manager_->GetShardSize(message_manager_->GetShardOfNode(config_.GetSelfInfo().id())); i++)
        replica_communicator_->SendMessage(*commit_request, message_manager_->GetNodesInShard(message_manager_->GetShardOfNode(config_.GetSelfInfo().id()))[i]);
    }

  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// If receive 2f+1 commit message, commit the request.
int Commitment::ProcessCommitMsg(std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  // If a non-coordinator recieves a message from outside the shard, retransmit to shard coord and end.
  if (! (message_manager_->NodesInSameShard(request->sender_id(), config_.GetSelfInfo().id()))
      && (config_.GetSelfInfo().id() != message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id()))) {
    
    uint32_t shard = message_manager_->GetShardOfNode(config_.GetSelfInfo().id());
    replica_communicator_->SendMessage(*request, message_manager_->GetPrimaryOfShard(shard));

    LOG(INFO) << "Subnode recieved message meant for coordinator, "
              << message_manager_->GetPrimaryOfNode(config_.GetSelfInfo().id());
    return -3;
  }
  if (context == nullptr || context->signature.signature().empty()) {
    LOG(ERROR) << "user request doesn't contain signature, reject"
               << " context:" << (context == nullptr);
    return -2;
  }
  if (request->is_recovery()) {
    return message_manager_->AddConsensusMsg(context->signature,
                                             std::move(request));
  }
  //global_stats_->IncCommit();
  // Add request to message_manager.
  // If it has received enough same requests(2f+1), message manager will
  // commit the request.

  // Altered for project 3.
  CollectorResultCode ret =
      message_manager_->AddConsensusMsg(context->signature, std::move(request));
  if (ret == CollectorResultCode::STATE_CHANGED) {
    
    // LOG(ERROR)<<request->data().size();
    // global_stats_->GetTransactionDetails(request->data());
    if (message_manager_->GetTransactionState(request->seq()) == TransactionStatue::READY_EXECUTE) {
      // (PHASE 5) In this case, we've actually performed a commit operation
      global_stats_->RecordStateTime("commit");
    }
    else  {
      // (PHASE 3) Move to local PBFT
      // broadcast a propose request to current shard's participants (excluding self)
      std::unique_ptr<Request> propose_request = resdb::NewRequest(
        Request::TYPE_PRE_PREPARE, *request, config_.GetSelfInfo().id());
      uint32_t shard = message_manager_->GetShardOfNode(config_.GetSelfInfo().id());
      for (int i = 0; i<message_manager_->GetShardSize(message_manager_->GetShardOfNode(config_.GetSelfInfo().id())); i++)
        if (message_manager_->GetNodesInShard(message_manager_->GetShardOfNode(config_.GetSelfInfo().id()))[i] != config_.GetSelfInfo().id())
        replica_communicator_->SendMessage(*propose_request, message_manager_->GetNodesInShard(message_manager_->GetShardOfNode(config_.GetSelfInfo().id()))[i]);
      
      // We also broadcast a prepare request here, because we've implicitly bypassed proposing the 
      // txn to ourselves.
      std::unique_ptr<Request> prepare_request = resdb::NewRequest(
        Request::TYPE_PREPARE, *request, config_.GetSelfInfo().id());
      for (int i = 0; i<message_manager_->GetShardSize(message_manager_->GetShardOfNode(config_.GetSelfInfo().id())); i++)
        replica_communicator_->SendMessage(*prepare_request, message_manager_->GetNodesInShard(message_manager_->GetShardOfNode(config_.GetSelfInfo().id()))[i]);
    }
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// =========== private threads ===========================
// If the transaction is executed, send back to the proxy.

// TODO: Only one shard replies to the client.
int Commitment::PostProcessExecutedMsg() {
  while (!stop_) {
    auto batch_resp = message_manager_->GetResponseMsg();
    if (batch_resp == nullptr) {
      continue;
    }
    global_stats_->SendSummary();
    if (message_manager_->NodesInSameShard(batch_resp->primary_id(), config_.GetSelfInfo().id())) {
      Request request;
      request.set_hash(batch_resp->hash());
      request.set_seq(batch_resp->seq());
      request.set_type(Request::TYPE_RESPONSE);
      request.set_sender_id(config_.GetSelfInfo().id());
      request.set_current_view(batch_resp->current_view());
      request.set_proxy_id(batch_resp->proxy_id());
      request.set_primary_id(batch_resp->primary_id());
      // LOG(ERROR)<<"send back to proxy:"<<batch_resp->proxy_id();
      batch_resp->SerializeToString(request.mutable_data());
      replica_communicator_->SendMessage(request, request.proxy_id());
    }
  }
  return 0;
}

DuplicateManager* Commitment::GetDuplicateManager() {
  return duplicate_manager_.get();
}

}  // namespace resdb
