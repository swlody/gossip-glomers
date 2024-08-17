build bin:
    cargo build --bin {{bin}}

echo: (build "echo")
    maelstrom test -w echo --bin ./target/debug/echo --node-count 1 --time-limit 10 --log-stderr

unique: (build "unique_ids")
    maelstrom test -w unique-ids --bin ./target/debug/unique_ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --log-stderr

broadcast nodes: (build "broadcast")
    maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count {{nodes}} --time-limit 20 --rate 10 --log-stderr

partition: (build "broadcast")
    maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition --log-stderr