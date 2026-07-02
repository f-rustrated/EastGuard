mod client_protocol;
mod client_sdk;
mod consumer_group_test;
mod lifecycle;

use crate::StartUp;
use crate::config::Environment;
use crate::it::helpers::default_env;

/// `(name, client_port, cluster/raft_port)` for one sim node.
pub(super) type NodeSpec = (&'static str, u16, u16);

/// Host a turmoil cluster: one `StartUp` per node in `nodes`, each wired with
/// `default_env`, its advertised host, and the other nodes as SWIM seeds (their
/// cluster ports). The node index passed to `default_env` is the 1-based position
/// in `nodes`. `configure` applies any per-test env tweaks (vnodes, segment limits,
/// pinned node-id suffix, …) before the node starts — pass `|_| {}` for defaults.
///
/// The host factory is `Fn` (turmoil re-invokes it on `bounce`), so every capture
/// is `Copy`; `configure` is too, so a bounced node restarts with the same config.
pub(super) fn host_cluster(
    sim: &mut turmoil::Sim,
    nodes: &'static [NodeSpec],
    configure: impl Fn(&mut Environment) + Copy + 'static,
) {
    for (i, &(name, cp, rp)) in nodes.iter().enumerate() {
        let idx = i as u32 + 1;
        sim.host(name, move || async move {
            let me = turmoil::lookup(name);
            let seeds: Vec<String> = nodes
                .iter()
                .filter(|(n, _, _)| *n != name)
                .map(|(n, _, peer_rp)| format!("{}:{}", turmoil::lookup(*n), peer_rp))
                .collect();
            let mut env = default_env(idx, name.to_string(), cp, rp);
            env.advertise_host = Some(me.to_string());
            env.join_seed_nodes = seeds;
            configure(&mut env);
            StartUp::with_env(env, 0).run().await?;
            Ok(())
        });
    }
}
