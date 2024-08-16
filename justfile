build:
    cargo build

echo:
    maelstrom test -w echo --bin ./target/debug/echo --nodes n1 --time-limit 10 --log-stderr