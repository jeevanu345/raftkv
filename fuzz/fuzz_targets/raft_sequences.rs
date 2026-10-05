#![no_main]
libfuzzer_sys::fuzz_target!(|data: &[u8]| {
    let mut sim = sim_tests::Sim::new(&[1, 2, 3], data.first().copied().unwrap_or(0) as u64);
    for byte in data.iter().take(500) {
        let id = (*byte as u64 % 3) + 1;
        match byte % 7 {
            0 => sim.crash(id),
            1 => sim.restart(id),
            2 => sim.nodes.get_mut(&id).unwrap().partitioned ^= true,
            3 => {
                if let Some(leader) = sim.leader() {
                    let _ = sim.propose(leader, vec![*byte]);
                }
            }
            _ => {}
        }
        sim.step();
    }
});
