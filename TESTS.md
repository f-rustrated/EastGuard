Tests to be added 

---

## 6. `tick()` — Direct Probe Timeout

| # | Scenario | Expected |
|---|---|---|
| 27 | `ACK_TIMEOUT_TICKS - 1` ticks after probe start | Probe still in Direct phase; no PingReqs sent |
| 28 | Exactly `ACK_TIMEOUT_TICKS` ticks, Ack never arrived | Direct probe removed; `start_indirect_probe` called; PingReqs sent to up to `INDIRECT_PING_COUNT` helpers; new probe entry for Indirect phase inserted |
| 29 | Direct timeout fires but target is already Dead (e.g. from gossip) | Indirect probe still starts. When Indirect times out, `try_mark_suspect` guard checks current state; if already Dead → no-op |
| 30 | Direct timeout fires but no live helper nodes exist | No PingReqs sent; node immediately marked Suspect; suspect timer started |
| 31 | Two probes both reach `ACK_TIMEOUT_TICKS` in the same tick | Both transition to Indirect in the same `tick()` call; both fire independently |

---

## 7. `tick()` — Indirect Probe Timeout

| # | Scenario | Expected |
|---|---|---|
| 32 | `INDIRECT_TIMEOUT_TICKS - 1` ticks after Indirect phase starts | Probe still Indirect; no state change |
| 33 | Exactly `INDIRECT_TIMEOUT_TICKS` ticks, no Ack | Probe removed; `try_mark_suspect` called; member transitions Alive → Suspect; `livenode_tracker` removes node; `suspect_timers.insert(node, SUSPECT_TIMEOUT_TICKS)` |
| 34 | Indirect timeout fires but member is already Alive (late Ack cleared it via `handle_incarnation_check`) | `try_mark_suspect` guard: `member.state == Alive` → no-op; node not suspected |
| 35 | Indirect timeout fires but member is already Suspect (concurrent probe already suspected them) | Guard: `member.state == Alive` → false → no-op; suspect timer not reset |
| 36 | Indirect timeout fires but member is already Dead | Guard → no-op |

---

## 8. `tick()` — Suspect Timeout

| # | Scenario | Expected |
|---|---|---|
| 37 | `SUSPECT_TIMEOUT_TICKS - 1` ticks after suspicion | Member still Suspect |
| 38 | Exactly `SUSPECT_TIMEOUT_TICKS` ticks | `try_mark_dead`: member transitions Suspect → Dead; removed from topology and live set |
| 39 | Suspect timer fires but member is now Alive (refuted via gossip during suspect period) | Guard: `member.state == Suspect` → false → no-op; node stays Alive |
| 40 | Suspect timer fires but member is already Dead | Guard → no-op |
| 41 | Two nodes are Suspect simultaneously; both timers expire in the same tick | Both marked Dead in the same `tick()` call |
| 42 | Node suspected, refutes (back to Alive), then fails again (suspected again) | Second `suspect_timers.insert` overwrites first (HashMap); suspect timer restarts from full duration |

---

## 9. SWIM Membership State Rules (`update_member`)

These test the SWIM precedence rules directly, independent of probe mechanics.

| # | Current State | Incoming Update | Expected Result |
|---|---|---|---|
| 43 | Alive(inc=1) | Alive(inc=2) | Alive(inc=2) — higher inc wins |
| 44 | Alive(inc=2) | Alive(inc=1) | Alive(inc=2) — lower inc ignored |
| 45 | Alive(inc=1) | Suspect(inc=1) | Suspect(inc=1) — same inc, Suspect > Alive |
| 46 | Alive(inc=1) | Suspect(inc=2) | Suspect(inc=2) — higher inc wins |
| 47 | Alive(inc=1) | Dead(inc=1) | Dead(inc=1) — same inc, Dead > Alive |
| 48 | Suspect(inc=1) | Alive(inc=1) | Suspect(inc=1) — same inc, Suspect > Alive (no refutation) |
| 49 | Suspect(inc=1) | Alive(inc=2) | Alive(inc=2) — higher inc refutes suspicion |
| 50 | Suspect(inc=1) | Suspect(inc=2) | Suspect(inc=2) — higher inc wins |
| 51 | Suspect(inc=1) | Dead(inc=1) | Dead(inc=1) — Dead wins at same inc |
| 52 | Suspect(inc=1) | Dead(inc=2) | Dead(inc=2) — higher inc wins |
| 53 | Dead(inc=1) | Alive(inc=1) | Dead(inc=1) — same inc, Dead is terminal |
| 54 | Dead(inc=1) | Alive(inc=2) | Alive(inc=2) — higher inc resurrects (false-death refutation) |
| 55 | Dead(inc=1) | Suspect(inc=1) | Dead(inc=1) — same inc, Dead wins |
| 56 | Dead(inc=1) | Suspect(inc=2) | Suspect(inc=2) — higher inc overrides Dead |
| 57 | Dead(inc=1) | Dead(inc=2) | Dead(inc=2) — higher inc wins |

---

## 10. Livenode Tracker Side Effects

`update_member` calls `livenode_tracker.update()`. These test the knock-on effects.

| # | Scenario | Expected |
|---|---|---|
| 58 | Gossip adds a new Alive node | Node appears in `livenode_tracker`; will be selected for probing |
| 59 | Node transitions Alive → Suspect | Removed from `livenode_tracker`; not probed in future rounds |
| 60 | Node transitions Alive → Dead | Removed from `livenode_tracker` |
| 61 | Node transitions Suspect → Alive (refutation) | Re-added to `livenode_tracker` at a random position |
| 62 | Node transitions Dead → Alive (higher incarnation) | Re-added to `livenode_tracker` |
| 63 | Node suspected and removed from live set; next probe round | Suspected node is never selected as probe target |
| 64 | Suspect refutes back to Alive; next probe round | Node eligible for selection again |

---

## 11. Topology Side Effects

| # | Scenario | Expected |
|---|---|---|
| 65 | Alive gossip for a new node | Node added to hash ring (`topology.update`) |
| 66 | Node transitions to Suspect | Removed from hash ring |
| 67 | Node transitions to Dead | Removed from hash ring |
| 68 | Dead node refuted (Alive with higher inc) | Re-added to hash ring |

---

## 12. Gossip Buffer

| # | Scenario | Expected |
|---|---|---|
| 69 | State change via `update_member` → `gossip_buffer.enqueue` | State change is included in the next outbound Ping or Ack's gossip list |
| 70 | Gossip for the same node twice in one period | Last-writer-wins via the SWIM rules; gossip buffer reflects final state |
| 71 | Gossip buffer included in Ack response | Ack gossip reflects updates from the Ping's own gossip (applied in Phase 1) |

---

## 13. `take_outbound()` Behaviour

| # | Scenario | Expected |
|---|---|---|
| 72 | Called twice with no intervening `step`/`tick` | First call returns all pending; second call returns empty vec |
| 75 | Direct timeout with `INDIRECT_PING_COUNT` available helpers | Up to `INDIRECT_PING_COUNT` `PingReq` packets |

---

## 14. Combined / Multi-Step Scenarios

These are the scenarios that were impossible to test with the old async design.

| # | Scenario | Steps |
|---|---|---|
| 78 | **Full failure detection lifecycle** | Add node-b. Tick to probe start. No Ack. Tick `ACK_TIMEOUT_TICKS` → Indirect. Tick `INDIRECT_TIMEOUT_TICKS` → Suspect. Tick `SUSPECT_TIMEOUT_TICKS` → Dead. |
| 79 | **Successful probe, no false positive** | Add node-b. Tick to probe start. Send Ack with matching seq. Tick `ACK_TIMEOUT_TICKS` → no transition; probe already gone. |
| 80 | **Ack saves node at the last tick** | Tick to probe start. Tick `ACK_TIMEOUT_TICKS - 1`. Step Ack. Tick 1 more → no transition (probe gone). |
| 81 | **Refutation during suspect period** | Fail probe → Suspect. Tick partway through suspect timer. Step Ping with self gossip claiming self Suspect → refute. Step gossip claiming node-b Alive(higher inc) → `update_member` → Alive. Tick remaining suspect timer → guard fires → no-op. |
| 82 | **Two concurrent probes, one succeeds one fails** | Tick to first probe (node-b). Tick to second probe (node-c). Send Ack for node-b. Tick `ACK_TIMEOUT_TICKS` → only node-c transitions to Indirect. Node-b unaffected. |
| 83 | **Gossip propagates a dead node before local detection** | Before local probe of node-b, receive Ping with gossip saying node-b Dead(inc=5). `update_member` → Dead. `livenode_tracker` removes node-b. When next tick tries to probe, node-b is never selected. |
| 84 | **Node suspected twice: timer restarts** | Fail probe → Suspect (timer=50). Gossip refutes → Alive. Fail probe again → Suspect. `suspect_timers.insert` overwrites → timer reset to 50. |
| 85 | **Self ping skipped** | Cluster has only self. Tick `PROTOCOL_PERIOD_TICKS` times. No Ping sent. |

---

## 15. Known Remaining Non-Determinism

These are NOT fixed by the `RawSwim` extraction and require separate work.

| Source | Location | Impact | Fix |
|---|---|---|---|
| Probe order | `LiveNodeTracker::add()` uses `StdRng::from_entropy()` | Cannot assert *which* node gets probed in multi-node tests | Seed the RNG; inject it into `LiveNodeTracker` |
| Gossip count | `gossip_buffer.collect()` may include variable entries depending on timing | Gossip list length may vary | Expose a deterministic `collect_n()` or seed |

For tests that are sensitive to probe order, **use a single-other-node cluster** until the RNG is made injectable.