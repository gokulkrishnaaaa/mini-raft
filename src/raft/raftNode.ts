import type { NodeState } from "./state.js";
import type { LogEntry } from "./type.js";
import type { RequestVoteRequest, RequestVoteResponse } from "./rpc.js";

export interface RaftNodeConfig {
  id: string;
  port: number;
  peers: string[];

  minElectionTimeout: number;
  maxElectionTimeout: number;
  heartbeatInterval: number;
}

class RaftNode {
  readonly id: string;
  readonly port: number;
  readonly peers: string[];

  readonly minElectionTimeout: number;
  readonly maxElectionTimeout: number;
  readonly heartbeatInterval: number;

  private state: NodeState = "FOLLOWER";
  private currentTerm: number = 0;
  private votedFor: string | null = null;
  private log: LogEntry[] = [];
  private commitIndex: number = -1;
  private lastApplied: number = -1;
  private votesReceived: number = 0;

  private electionTimer?: ReturnType<typeof setTimeout> | undefined;
  private heartbeatTimer?: ReturnType<typeof setTimeout> | undefined;

  constructor(config: RaftNodeConfig) {
    this.id = config.id;
    this.port = config.port;
    this.peers = config.peers;
    this.minElectionTimeout = config.minElectionTimeout;
    this.maxElectionTimeout = config.maxElectionTimeout;
    this.heartbeatInterval = config.heartbeatInterval;
  }

  private getRandomElectionTimeout(): number {
    return (
      Math.floor(
        Math.random() * (this.maxElectionTimeout - this.minElectionTimeout + 1),
      ) + this.minElectionTimeout
    );
  }

  private startElectionTimer() {
    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
    }
    const timeout = this.getRandomElectionTimeout();

    this.electionTimer = setTimeout(() => {
      this.startElection();
    }, timeout);
  }

  private stopElectionTimer() {
    if (this.electionTimer) {
      clearTimeout(this.electionTimer);
    }
    this.electionTimer = undefined;
  }

  private startHeartbeatTimer() {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }
    this.heartbeatTimer = setTimeout(() => {
      this.sendHeartbeat();
      this.startHeartbeatTimer();
    }, this.heartbeatInterval);
  }

  private stopHeartbeatTimer() {
    if (this.heartbeatTimer) {
      clearTimeout(this.heartbeatTimer);
    }
    this.heartbeatTimer = undefined;
  }

  start() {
    this.startElectionTimer();
  }

  private getLastLogIndex(): number {
    return this.log.length - 1;
  }

  private getLastLogTerm(): number {
    if (this.log.length === 0) return 0;
    return this.log[this.log.length - 1]!.term;
  }

  private isCandidateLogUpToDate(lastLogIndex: number, lastLogTerm: number): boolean {
    const myLastTerm = this.getLastLogTerm();
    const myLastIndex = this.getLastLogIndex();
    if (lastLogTerm !== myLastTerm) return lastLogTerm > myLastTerm;
    return lastLogIndex >= myLastIndex;
  }

  private startElection() {
    this.stopHeartbeatTimer();
    this.state = "CANDIDATE";
    this.currentTerm++;
    this.votedFor = this.id;
    this.votesReceived = 1;
    this.startElectionTimer();
  }

  private becomeFollower(newTerm: number) {
    this.state = "FOLLOWER";
    this.currentTerm = newTerm;
    this.votedFor = null;
    this.votesReceived = 0;
    this.stopHeartbeatTimer();
    this.startElectionTimer();
  }

  private becomeLeader() {
    this.state = "LEADER";
    this.stopElectionTimer();
    this.votesReceived = 0;
    this.sendHeartbeat();
    this.startHeartbeatTimer();
  }

  private sendHeartbeat() {
    console.log(`${this.id} sending heartbeat`);
  }

  handleRequestVote(request: RequestVoteRequest): RequestVoteResponse {
    if (request.term < this.currentTerm) {
      return { term: this.currentTerm, voteGranted: false };
    }

    if (request.term > this.currentTerm) {
      this.becomeFollower(request.term);
    }

    const alreadyVotedForAnother =
      this.votedFor !== null && this.votedFor !== request.candidateId;

    if (alreadyVotedForAnother) {
      return { term: this.currentTerm, voteGranted: false };
    }

    if (!this.isCandidateLogUpToDate(request.lastLogIndex, request.lastLogTerm)) {
      return { term: this.currentTerm, voteGranted: false };
    }

    this.votedFor = request.candidateId;
    this.startElectionTimer();

    return { term: this.currentTerm, voteGranted: true };
  }
}
