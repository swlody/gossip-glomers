build:
    @find src -name "*.rs"
    cargo build

run:
    maelstrom test -w echo --bin ./target/debug/gossip_glomers --nodes n1 --time-limit 10 --log-stderr