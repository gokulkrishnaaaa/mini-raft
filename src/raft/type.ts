export interface LogEntry {
    index: number;
    term: number;
    command: string;
}