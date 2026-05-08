# Decoder mappings

YAML mappings consumed by `core.adapters.evm.decoders.generic.GenericABIDecoder`.
One file per protocol. Each file may declare multiple events.

## Schema

```yaml
protocol: <slug>                      # e.g. aave_v3, uniswap_v3
chains: [ethereum, base, arbitrum, optimism, polygon]
events:
  - name: Swap                        # event name (no parens)
    inputs:
      - {name: sender,    type: address, indexed: true}
      - {name: amount,    type: uint256}
      - {name: recipient, type: address, indexed: true}
    canonical:                        # bindings to CanonicalEvent fields
      event_type: <literal or "{arg}">
      entity_id:  "{arg-or-log-field}"
      venue:      "{log.address}"
      token_in:   "{arg}"             # optional — omit if not applicable
      token_out:  "{arg}"
      amount_in:  "{arg}"             # decimal/int passes through; plugin can override
      amount_out: "{arg}"
      counterparty: "{arg}"
      link_key:     "{arg}"
      link_key_type: <literal>
      aggregator:    <literal>
      extra:                          # JSON-serialisable map of extra fields
        some_key: "{arg}"
    plugin: "module.path:fn_name"     # optional post-processor
```

The event's canonical signature (used for topic0 derivation) is computed
from `name` + the ordered `inputs` types: `Swap(address,uint256,address)`.
Indexed args may appear at any position — they don't have to be contiguous.
UniV2's `Swap` is the canonical mixed example: `sender` indexed at position 0
and `to` indexed at position 5.

## Field bindings

| Form                | Resolves to                         |
| ------------------- | ----------------------------------- |
| `"swap"`            | literal string                      |
| `"{name}"`          | `args[name]` (decoded event arg)    |
| `"{log.address}"`   | the log's contract address          |
| `"{log.transactionHash}"` | the tx hash                  |
| `42` / `null`       | literal int / None                  |

## Plugins — when and how

Use a plugin when a field cannot be expressed by a single arg lookup. Common cases:

- **Sign-based amount selection** (UniV3 amount0/amount1 are signed; the
  positive one is "in", the negative one is "out").
- **Pool-token resolution** (UniV2/V3 emit amounts but not which token is
  token0/token1 — needs a pool registry lookup; will be wired in via the
  pool_registry pipeline).
- **Multi-event dispatch** (GMX V2's EventEmitter encodes the actual
  event name in `topic2`; the plugin reads it and rebrands `event_type`).
- **Array decoding** (Seaport's offer/consideration arrays).

Plugin signature:

```python
def my_plugin(
    event: DecodedEvent,
    args: dict[str, Any],
    log: dict[str, Any],
) -> DecodedEvent:
    ...
```

The plugin mutates and returns the event. `args` contains decoded inputs by
name, `log` is the raw HyperSync/JSON-RPC log dict.

Plugins live in `core/adapters/evm/decoders/plugins.py`. Reference them in
YAML as `core.adapters.evm.decoders.plugins:fn_name`.
