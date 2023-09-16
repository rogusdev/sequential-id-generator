# sequential-id-generator

A service to generate ids on request, and keep them alive, for distributing across clients -- ideally as process ids used in generating unique ids inside those clients (ala uuid machine id), etc

4 config env vars:
- "PORT" -- default 3000
- "MAX" -- default 65535
- "MIN" -- default 1
- "TIMEOUT" -- default 2000

It's a very straightforward rust project, all the basics get you started with the code:

        cargo run
        cargo test

        curl localhost:3000/next
        curl localhost:3000/heartbeat/1
