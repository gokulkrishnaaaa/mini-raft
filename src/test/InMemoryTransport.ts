import type { RaftTransport } from "../raft/transport.js";
import type { RaftNode } from "../raft/raftNode.js";
import type {
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
} from "../raft/rpc.js";

class InMemoryNetwork {
  private nodes: Map<string, RaftNode> = new Map();
  private disconnected: Set<string> = new Set();
  readonly networkDelay: number;

  constructor(networkDelay: number = 5) {
    this.networkDelay = networkDelay;
  }

  registerNode(id: string, node: RaftNode) {
    this.nodes.set(id, node);
  }

  getNode(id: string): RaftNode | undefined {
    return this.nodes.get(id);
  }

  disconnect(nodeId: string) {
    this.disconnected.add(nodeId);
  }

  reconnect(nodeId: string) {
    this.disconnected.delete(nodeId);
  }

  isConnected(nodeId: string): boolean {
    return !this.disconnected.has(nodeId);
  }

  canCommunicate(fromId: string, toId: string): boolean {
    return this.isConnected(fromId) && this.isConnected(toId);
  }
}

export class InMemoryTransport implements RaftTransport {
  private network: InMemoryNetwork;
  private sourceId: string;

  constructor(network: InMemoryNetwork, sourceId: string) {
    this.network = network;
    this.sourceId = sourceId;
  }

  async sendRequestVote(
    peerId: string,
    request: RequestVoteRequest
  ): Promise<RequestVoteResponse> {
    if (!this.network.canCommunicate(this.sourceId, peerId)) {
      throw new Error(`Cannot reach ${peerId} from ${this.sourceId}`);
    }

    const node = this.network.getNode(peerId);
    if (!node) {
      throw new Error(`Node ${peerId} does not exist`);
    }

    await this.simulateNetworkDelay();

    if (!this.network.canCommunicate(this.sourceId, peerId)) {
      throw new Error(`Connection lost to ${peerId}`);
    }

    return node.handleRequestVote(request);
  }

  async sendAppendEntries(
    peerId: string,
    request: AppendEntriesRequest
  ): Promise<AppendEntriesResponse> {
    if (!this.network.canCommunicate(this.sourceId, peerId)) {
      throw new Error(`Cannot reach ${peerId} from ${this.sourceId}`);
    }

    const node = this.network.getNode(peerId);
    if (!node) {
      throw new Error(`Node ${peerId} does not exist`);
    }

    await this.simulateNetworkDelay();

    if (!this.network.canCommunicate(this.sourceId, peerId)) {
      throw new Error(`Connection lost to ${peerId}`);
    }

    return node.handleAppendEntries(request);
  }

  private simulateNetworkDelay(): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, this.network.networkDelay));
  }
}

export { InMemoryNetwork };
