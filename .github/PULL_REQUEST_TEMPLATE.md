## Summary

<!-- What does this change and why? -->

## Checklist

- [ ] `cargo test` passes from `cache/`
- [ ] New commands follow the steps in `ARCHITECTURE.md` (protocol → storage → commands → server dispatch → tests)
- [ ] Write commands go through `is_write_command` / `apply_write_command` so WAL replay and replication stay in sync
- [ ] Docs / README updated if behaviour or config changed
