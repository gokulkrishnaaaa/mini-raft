import type { NodeState } from "./state.js";
import type { LogEntry } from "./type.js";

export interface RaftNodeConfig {
  id: string;
  port: number;
  peers: string[];

  minElectionTimeout: number;
  maxElectionTimeout: number;
}

interface RequestVoteRequest {
  term: number;
  candidateId: string;
  lastLogIndex: number;
  lastLogTerm: number;
}

interface RequestVoteResponse {
  term: number;
  voteGranted: boolean;
}

class RaftNode {
  readonly id: string;
  readonly port: number;
  readonly peers: string[];

  readonly minElectionTimeout: number;
  readonly maxElectionTimeout: number;

  private state: NodeState = "FOLLOWER";
  private currentTerm: number = 0;
  private votedFor: string | null = null;
  private log: LogEntry[] = [];
  private commitIndex: number = -1;
  private lastApplied: number = -1;
  private votesReceived: number | null = null;

  private electionTimer?: ReturnType<typeof setTimeout>;
  private heartbeatTimer?: ReturnType<typeof setTimeout>;

  constructor(config: RaftNodeConfig) {
    this.id = config.id;
    this.port = config.port;
    this.peers = config.peers;
    this.minElectionTimeout = config.minElectionTimeout;
    this.maxElectionTimeout = config.maxElectionTimeout;
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

  private startElection() {
    this.state = "CANDIDATE";
    this.currentTerm++;
    this.votedFor = this.id;
    this.votesReceived = 1;
    this.startElectionTimer();
  }

  private becomeFollower(newTerm: number) {
    this.state = "FOLLOWER";
    this.currentTerm = newTerm;
    this.votesReceived = null;
    this.votedFor = null;
    this.startElectionTimer();
  }

  private becomeLeader() {
    this.state = "LEADER";
    this.startElectionTimer();
  }
}
