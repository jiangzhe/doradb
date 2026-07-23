# Checkpoint and Recovery

The former combined checkpoint-and-recovery design has been split into two
domain-focused documents:

- [Checkpoint](./checkpoint.md) describes how Doradb publishes durable table
  and catalog state.
- [Recovery](./recovery.md) describes how Doradb loads that state and replays
  redo during restart.

The checkpoint document links to the data-checkpoint and deletion-checkpoint
deep dives where their internal workflows matter.
