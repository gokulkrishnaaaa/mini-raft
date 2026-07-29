import type {
  RequestVoteRequest,
  RequestVoteResponse,
  AppendEntriesRequest,
  AppendEntriesResponse,
} from "./rpc.js";

export interface RaftTransport {
  sendRequestVote(
    peerId: string,
    request: RequestVoteRequest
  ): Promise<RequestVoteResponse>;

  sendAppendEntries(
    peerId: string,
    request: AppendEntriesRequest
  ): Promise<AppendEntriesResponse>;
}
