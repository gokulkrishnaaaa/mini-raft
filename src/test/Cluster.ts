import { RaftNode } from "../raft/raftNode.js";
import { InMemoryTransport, InMemoryNetwork } from "./InMemoryTransport.js";

export interface ClusterConfig {
  nodeCount: number;
  minElectionTimeout: number;
  maxElectionTimeout: number;
  heartbeatInterval: number;
}

export class Cluster {
  private nodes: Map<string, RaftNode> = new Map();
  private network: InMemoryNetwork;
  private appliedCommands: Map<string, string[]> = new Map();

  constructor(config: ClusterConfig) {
    this.network = new InMemoryNetwork();

    const nodeIds = Array.from({ length: config.nodeCount }, (_, i) => `node-${i + 1}`);

    for (const id of nodeIds) {
      const peers = nodeIds.filter((peerId) => peerId !== id);
      const applied: string[] = [];
      this.appliedCommands.set(id, applied);

      const transport = new InMemoryTransport(this.network, id);

      const node = new RaftNode({
        id,
        port: 0,
        peers,
        minElectionTimeout: config.minElectionTimeout,
        maxElectionTimeout: config.maxElectionTimeout,
        heartbeatInterval: config.heartbeatInterval,
        transport,
        onApply: (index, command) => {
          applied.push(command);
          console.log(`[${id}] Applied index=${index} command="${command}"`);
        },
      });

      this.nodes.set(id, node);
      this.network.registerNode(id, node);
    }
  }

  start() {
    for (const node of this.nodes.values()) {
      node.start();
    }
  }

  stop() {
    for (const node of this.nodes.values()) {
      node.stop();
    }
  }

  getNode(id: string): RaftNode | undefined {
    return this.nodes.get(id);
  }

  getAllNodes(): RaftNode[] {
    return Array.from(this.nodes.values());
  }

  getLeader(): RaftNode | undefined {
    for (const node of this.nodes.values()) {
      if (node.getStatus().state === "LEADER") {
        return node;
      }
    }
    return undefined;
  }

  getFollowers(): RaftNode[] {
    return this.getAllNodes().filter((n) => n.getStatus().state === "FOLLOWER");
  }

  disconnect(nodeId: string) {
    this.network.disconnect(nodeId);
    console.log(`[CLUSTER] Disconnected ${nodeId}`);
  }

  reconnect(nodeId: string) {
    this.network.reconnect(nodeId);
    console.log(`[CLUSTER] Reconnected ${nodeId}`);
  }

  printStatus() {
    console.log("\n--- Cluster Status ---");
    for (const node of this.nodes.values()) {
      const status = node.getStatus();
      const connected = this.network.isConnected(status.id) ? "✓" : "✗";
      console.log(
        `${status.id}: ${status.state.padEnd(9)} term=${status.term} log=${status.logLength} commit=${status.commitIndex} [${connected}]`
      );
    }
    console.log("----------------------\n");
  }

  getAppliedCommands(nodeId: string): string[] {
    return this.appliedCommands.get(nodeId) ?? [];
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
