# Backlog: Audit buffer page reinterpretation casts

## Summary

Audit the buffer pool's generic page reinterpretation path separately from the bytemuck-to-zerocopy replacement. The current buffer guard layer reinterprets arena-owned page bytes as caller-selected BufferPage types through raw pointers.

## Reference

Discovered while planning the bytemuck-to-zerocopy replacement. Relevant starting points: doradb-storage/src/buffer/guard.rs page_ref/page_mut, doradb-storage/src/buffer/page.rs BufferPage, doradb-storage/src/buffer/arena.rs init_page, doradb-storage/src/buffer/util.rs initialize_frame_and_page_arrays.

## Deferred From (Optional)


## Deferral Context (Optional)


## Scope Hint

Focus on the buffer pool's raw Page-to-T reinterpretation invariants, alignment guarantees, lifetime/latch coupling, and whether zerocopy traits or stronger layout assertions can make the contract more explicit. Keep direct bytemuck replacement and storage endianness fixes out of this follow-up.

## Acceptance Hint

The future task should document the unsafe contract, add or tighten size/alignment/layout assertions for BufferPage implementors, evaluate whether any casts can use zerocopy APIs without harming RowPage atomics and hot paths, and run the unsafe review checklist if unsafe code changes.

## Notes (Optional)

Classified as a standalone follow-up backlog item. It was intentionally kept out of the current bytemuck replacement task so that task remains focused on direct bytemuck usage and little-endian storage layout fixes.

## Close Reason (Added When Closed)

When a backlog item is moved to `docs/backlogs/closed/`, append:

```md
## Close Reason

- Type: <implemented|stale|replaced|duplicate|wontfix|already-implemented|other>
- Detail: <reason detail>
- Closed By: <backlog close>
- Reference: <task/issue/pr reference>
- Closed At: <YYYY-MM-DD>
```
