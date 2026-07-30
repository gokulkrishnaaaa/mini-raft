import { Cluster, sleep } from "./Cluster.js";

async function main() {
  console.log("=== Mini-Raft Test Suite ===\n");

  // Scenario 1: Leader Election
  console.log("📋 Scenario 1: Leader Election");
  console.log("Starting a 3-node cluster...\n");

  const cluster = new Cluster({
    nodeCount: 3,
    minElectionTimeout: 150,
    maxElectionTimeout: 300,
    heartbeatInterval: 50,
  });

  cluster.start();

  // Wait for election to complete
  await sleep(500);
  cluster.printStatus();

  const leader = cluster.getLeader();
  if (leader) {
    console.log(`✅ Leader elected: ${leader.getStatus().id}\n`);
  } else {
    console.log("❌ No leader elected!\n");
    cluster.stop();
    return;
  }

  // Scenario 2: Log Replication
  console.log("📋 Scenario 2: Log Replication");
  console.log("Proposing commands to the leader...\n");

  leader.propose("SET x = 1");
  leader.propose("SET y = 2");
  leader.propose("SET z = 3");

  // Wait for replication
  await sleep(300);
  cluster.printStatus();

  // Check all nodes have applied the commands
  console.log("Applied commands per node:");
  for (const node of cluster.getAllNodes()) {
    const applied = cluster.getAppliedCommands(node.id);
    console.log(`  ${node.id}: [${applied.join(", ")}]`);
  }
  console.log();

  // Scenario 3: Leader Failure & Re-election
  console.log("📋 Scenario 3: Leader Failure & Re-election");
  const oldLeaderId = leader.getStatus().id;
  console.log(`Disconnecting leader (${oldLeaderId})...\n`);

  cluster.disconnect(oldLeaderId);

  // Wait for new election
  await sleep(600);
  cluster.printStatus();

  const newLeader = cluster.getLeader();
  if (newLeader && newLeader.id !== oldLeaderId) {
    console.log(`✅ New leader elected: ${newLeader.getStatus().id}\n`);

    // Propose more commands to new leader
    console.log("Proposing more commands to new leader...\n");
    newLeader.propose("SET a = 10");
    newLeader.propose("SET b = 20");

    await sleep(300);
    cluster.printStatus();
  } else {
    console.log("❌ No new leader elected!\n");
  }

  // Scenario 4: Reconnect old leader
  console.log("📋 Scenario 4: Reconnect Old Leader");
  console.log(`Reconnecting ${oldLeaderId}...\n`);

  cluster.reconnect(oldLeaderId);

  // Wait for it to catch up
  await sleep(500);
  cluster.printStatus();

  const oldLeaderStatus = cluster.getNode(oldLeaderId)?.getStatus();
  if (oldLeaderStatus?.state === "FOLLOWER") {
    console.log(`✅ Old leader ${oldLeaderId} stepped down to follower\n`);
  }

  // Final state
  console.log("📋 Final Applied Commands:");
  for (const node of cluster.getAllNodes()) {
    const applied = cluster.getAppliedCommands(node.id);
    console.log(`  ${node.id}: [${applied.join(", ")}]`);
  }

  console.log("\n=== Test Complete ===");
  cluster.stop();
}

main().catch(console.error);
