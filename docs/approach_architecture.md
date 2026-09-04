PLUG-AND-PLAY OFFLINE SYNC PLATFORM

                       Public SDK API
                             │
                   ┌─────────▼─────────┐
                   │     Core Engine   │
                   │                   │
                   │ Transactions      │
                   │ Outbox            │
                   │ Push/Pull         │
                   │ Retry             │
                   │ Idempotency       │
                   │ Checkpoints       │
                   │ Conflict Engine   │
                   └────┬────┬────┬───┘
                        │    │    │
                PORTS / CONTRACTS
                        │    │    │
           ┌────────────┘    │    └─────────────┐
           ▼                 ▼                  ▼
     Local Store        Transport         Conflict Policy
        Port               Port                Port
           │                 │                  │
        SQLite             HTTP              Version
        Adapter            gRPC              LWW
                                             Custom

                             │
                             ▼
                       Sync Gateway
                             │
                      Remote Store Port
                             │
            ┌────────────────┼────────────────┐
            ▼                ▼                ▼
        PostgreSQL          MySQL            Custom
         Adapter            Adapter          Adapter