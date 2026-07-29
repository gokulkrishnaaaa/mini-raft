import type { NodeState } from "./state.js";
import type { LogEntry } from "./type.js";
import type {
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
} from "./rpc.js";
import type { RaftTransport } from "./transport.js";

export interface RaftNodeConfig {
  id: string;
  port: number;
  peers: string[];

  minElectionTimeout: number;
  maxElectionTimeout: number;
  heartbeatInterval: number;

  transport: RaftTransport;
}

export class RaftNode {
  readonly id: string;
  readonly port: number;
  readonly peers: string[];

  readonly minElectionTimeout: number;
  readonly maxElectionTimeout: number;
  readonly heartbeatInterval: number;

  private readonly transport: RaftTransport;

  private state: NodeState = "FOLLOWER";
  private currentTerm: number = 0;
  private votedFor: string | null = null;
  private log: LogEntry[] = [];
  private commitIndex: number = -1;
  private lastApplied: number = -1;
  private votesReceived: number = 0;

  private nextIndex: Map<string, number> = new Map();
  private matchIndex: Map<string, number> = new Map();

  private electionTimer?: ReturnType<typeof setTimeout> | undefined;
  private heartbeatTimer?: ReturnType<typeof setTimeout> | undefined;

  constructor(config: RaftNodeConfig) {
    this.id = config.id;
    this.port = config.port;
    this.peers = config.peers;
    this.minElectionTimeout = config.minElectionTimeout;
    this.maxElectionTimeout = config.maxElectionTimeout;
    this.heartbeatInterval = config.heartbeatInterval;
    this.transport = config.transport;
  }

  private getClusterSize(): number {
    return this.peers.length + 1;
  }

  private getQuorum(): number {
    return Math.floor(this.getClusterSize() / 2) + 1;
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

  private getLogTerm(index: number): number {
    if (index < 0 || index >= this.log.length) return 0;
    return this.log[index]!.term;
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

    const electionTerm = this.currentTerm;

    const request: RequestVoteRequest = {
      term: electionTerm,
      candidateId: this.id,
      lastLogIndex: this.getLastLogIndex(),
      lastLogTerm: this.getLastLogTerm(),
    };

    for (const peer of this.peers) {
      this.transport
        .sendRequestVote(peer, request)
        .then((response) => {
          this.handleRequestVoteResponse(electionTerm, response);
        })
        .catch(() => {
          // Network failure
        });
    }
  }

  private handleRequestVoteResponse(
    electionTerm: number,
    response: RequestVoteResponse
  ) {
    // if our term or state changed since we sent the request,this response is no longer relevant.
    if (this.currentTerm !== electionTerm || this.state !== "CANDIDATE") {
      return;
    }

    if (response.term > this.currentTerm) {
      this.becomeFollower(response.term);
      return;
    }

    if (response.voteGranted) {
      this.votesReceived++;

      if (this.votesReceived >= this.getQuorum()) {
        this.becomeLeader();
      }
    }
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

    const lastLogIndex = this.getLastLogIndex();
    for (const peer of this.peers) {
      this.nextIndex.set(peer, lastLogIndex + 1);
      this.matchIndex.set(peer, -1);
    }

    this.sendHeartbeat();
    this.startHeartbeatTimer();
  }

  private sendHeartbeat() {
    if (this.state !== "LEADER") return;

    const currentTerm = this.currentTerm;

    for (const peer of this.peers) {
      const nextIdx = this.nextIndex.get(peer) ?? 0;
      const prevLogIndex = nextIdx - 1;
      const prevLogTerm = this.getLogTerm(prevLogIndex);

      const request: AppendEntriesRequest = {
        term: currentTerm,
        leaderId: this.id,
        prevLogIndex,
        prevLogTerm,
        entries: [],
        leaderCommit: this.commitIndex,
      };

      this.transport
        .sendAppendEntries(peer, request)
        .then((response) => {
          this.handleAppendEntriesResponse(peer, currentTerm, request, response);
        })
        .catch(() => {
          // Network failure
        });
    }
  }

  private handleAppendEntriesResponse(
    peer: string,
    sentTerm: number,
    request: AppendEntriesRequest,
    response: AppendEntriesResponse
  ) {
    if (this.currentTerm !== sentTerm || this.state !== "LEADER") {
      return;
    }

    if (response.term > this.currentTerm) {
      this.becomeFollower(response.term);
      return;
    }

    if (response.success) {
      const newMatchIndex = request.prevLogIndex + request.entries.length;
      this.matchIndex.set(peer, newMatchIndex);
      this.nextIndex.set(peer, newMatchIndex + 1);
    } else {
      const currentNextIndex = this.nextIndex.get(peer) ?? 0;
      if (currentNextIndex > 0) {
        this.nextIndex.set(peer, currentNextIndex - 1);
      }
    }
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

  handleAppendEntries(request: AppendEntriesRequest): AppendEntriesResponse {
    if (request.term < this.currentTerm) {
      return { term: this.currentTerm, success: false };
    }

    if (request.term > this.currentTerm) {
      this.becomeFollower(request.term);
    } else if (this.state === "CANDIDATE") {
      this.becomeFollower(request.term);
    }

    this.startElectionTimer();

    if (request.prevLogIndex >= 0) {
      if (request.prevLogIndex >= this.log.length) {
        return { term: this.currentTerm, success: false };
      }
      if (this.log[request.prevLogIndex]!.term !== request.prevLogTerm) {
        return { term: this.currentTerm, success: false };
      }
    }

    let insertIndex = request.prevLogIndex + 1;
    for (const entry of request.entries) {
      if (insertIndex < this.log.length) {
        if (this.log[insertIndex]!.term !== entry.term) {
          this.log.splice(insertIndex);
        }
      }
      if (insertIndex >= this.log.length) {
        this.log.push(entry);
      }
      insertIndex++;
    }

    if (request.leaderCommit > this.commitIndex) {
      const lastNewEntryIndex = request.prevLogIndex + request.entries.length;
      this.commitIndex = Math.min(request.leaderCommit, lastNewEntryIndex);
    }

    return { term: this.currentTerm, success: true };
  }
}
