# Can run with '--set nemesis [nemesis]'
nemesis := ""
nemesis_arg := if nemesis != "" {
    "--nemesis " + nemesis
} else {
    ""
}

build bin:
    cargo build --bin {{bin}}

echo: (build "echo")
    maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --rate 100 --time-limit 1 --log-stderr

unique: (build "unique_ids")
    maelstrom test -w unique-ids --bin ./target/debug/unique_ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --log-stderr

broadcast nodes="1": (build "broadcast")
    maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count {{nodes}} --time-limit 20 --rate 10 {{nemesis_arg}} --log-stderr

efficiency: (build "broadcast")
    maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 {{nemesis_arg}} --log-stderr

counter: (build "counter")
    maelstrom test -w g-counter --bin ./target/debug/counter --node-count 3 --rate 100 --time-limit 20 {{nemesis_arg}} --log-stderr