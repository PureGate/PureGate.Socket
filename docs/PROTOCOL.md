# PureGate.Socket wire protocol

This describes the bytes PureGate.Socket peers exchange over one TCP connection (optionally inside TLS). The handshake carries the protocol version (currently 1), so peers built from mismatched sources fail at connect time instead of misreading frames.

## Encoding primitives

| Name | Encoding |
|---|---|
| `u8` | One byte. |
| `uint32-be` | Four bytes, big-endian. |
| `varint` | Unsigned LEB128: 7 bits per byte, least significant group first, high bit set on every byte but the last. At most 10 bytes. |
| `zigzag` | A signed 64-bit integer mapped to unsigned (`(n << 1) ^ (n >> 63)`) and written as a `varint`. Small negative numbers stay short. |
| `str` | Byte length as a `varint`, then that many bytes of UTF-8. |

## Frames

```
frame = length:uint32-be  body          length = number of bytes in body, 2 to MaxFrameSizeBytes
body  = kind:u8  flags:u8  id:zigzag  [fields...]  payload
```

The **payload** is everything left in the body after the fields; it has no length of its own.

### Flags

Each bit marks a field as present, or carries a boolean. Fields appear in this order:

| Bit | Name | Meaning |
|---|---|---|
| 0 | Compressed | The payload is Brotli-compressed. Only valid if both sides enabled compression. |
| 1 | WindowExhausted | On a stream item: the producer has no window left after this frame (see Flow control). |
| 2 | Ok | Success. Meaningful on responses, stream starts and ends, and handshake frames. |
| 3 | HasMethod | A `method` field follows. |
| 4 | HasDeadline | A `deadline:varint` field follows, in milliseconds. |
| 5 | HasIdempotencyKey | An `idempotency-key:str` field follows. |
| 6 | HasValue | A `value:zigzag` field follows. Its meaning depends on the kind. |
| 7 | HasError | An `error` field follows. |

Chunk frames additionally carry `original-kind:u8 index:varint count:varint total:varint` after the flagged fields.

### Method

```
method = code:varint  [service:str  operation:str]
```

| `code` | Meaning |
|---|---|
| `0` | Literal: the names follow. |
| odd, `2n + 1` | Definition: method `n` is these names (which follow). |
| even, `2n`, `n ≥ 1` | Reference to method `n`; no names follow. |

Each direction of a connection has its own method table. Definitions must be numbered 1, 2, 3, … in the order they are sent; a receiver rejects a definition out of order or past 4,096 entries, and a reference to a number not yet defined. A sender that has defined 4,096 methods sends further names literally. Names are at most 1,024 bytes each. Handshake frames always use literal names, and so do frames that are sent in chunks.

### Error

```
error = code-index:varint  [code:str]  message:str
```

`code-index` 0 means a custom code, written as text. Otherwise it selects one of the library's codes:

| Index | Code |
|---|---|
| 1 | `timeout` |
| 2 | `cancelled` |
| 3 | `unauthorized` |
| 4 | `unsupported_version` |
| 5 | `unsupported_operation` |
| 6 | `payload_too_large` |
| 7 | `protocol_error` |
| 8 | `handler_failed` |
| 9 | `connection_closed` |
| 10 | `overloaded` |

## Frame kinds

| Kind | Name | Id | Fields used | Payload |
|---|---|---|---|---|
| 1 | Request | sender's request id | method, deadline, idempotency key | request body |
| 2 | Response | id of the request | Ok, error | response body |
| 3 | Event | sender's message id | method | event body |
| 4 | Ping | any; `-2` is a liveness probe | | |
| 5 | Pong | id of the ping | | |
| 6 | Close | | | |
| 7 | Cancel | id of the request or stream to cancel | | |
| 8 | Chunk | id of the chunked frame | chunk fields | a slice of the chunked frame's body |
| 9 | StreamRequest | sender's stream id | method, deadline, value = initial stream window in bytes | request body |
| 10 | StreamStart | id of the stream | Ok, error | |
| 11 | StreamItem | id of the stream | WindowExhausted | stream data |
| 12 | StreamEnd | id of the stream | Ok, error | |
| 15 | Credit | `0` for message credits, otherwise a stream id | value = amount | |

Ids are chosen by the side that starts an exchange; responses, stream frames and cancels echo them. The two directions of a connection number independently.

## Handshake

All handshake frames are Events with service `socket`, sent with literal names. Payloads are JSON.

1. Client → server: `hello`, payload `{ version, nodeId, nonce }`.
2. Server → client: `challenge`, Ok set, payload `{ version, nodeId, nonce, signature }`.
3. Client → server: `proof`, value = the client's message window, payload `{ version, nodeId, signature }`.
4. Server → client: `ready`, Ok set, value = the server's message window.

A failure at any step is answered with `ready` without Ok and with an error, then the connection closes. The version in `hello` and `challenge` must match the receiver's. Until `ready`, frames are limited to `MaxHandshakeFrameBytes` (16 KB by default) in both directions; after it, to `MaxFrameSizeBytes`.

The signatures are produced by the configured authenticator. `HmacSocketAuthenticator` signs, with HMAC-SHA256 and the secret the two nodes share, the concatenation of these fields, each as a 4-byte big-endian length followed by its UTF-8 bytes: `PureGate.Socket.v1`, `challenge` or `proof`, the version in decimal, the signer's node id, the other node's node id, the client nonce, the server nonce. Signatures are base64.

## Chunking

A frame whose body exceeds the receiver's frame limit is sent as consecutive Chunk frames. The sender encodes the frame's body once (header and payload), then splits those bytes into `count` chunks of equal size except the last; `total` is the body's length. Chunks of one frame are sent back to back and in order.

The receiver concatenates the chunks and decodes the result as an ordinary frame body, whose kind and id must match the chunks. It rejects chunks out of order or inconsistent with the first one, and drops the remaining chunks of a frame that has made no progress for `ChunkTimeout`. The memory held by partly received frames is capped per connection.

## Flow control

**Message credits.** During the handshake each side learns the other's message window: how many requests, events and stream requests it may have waiting at the other side. Sending one of these costs one credit. When the receiver's handlers pick messages up, it returns credits with a Credit frame (id 0, value = number of credits), in batches of about a quarter of its window. A sender with no credit waits. A receiver whose queue would overflow treats it as a protocol violation.

**Stream credits.** A StreamRequest carries the consumer's initial window in bytes. The producer may send only as many stream bytes as it has been granted; the consumer grants more with Credit frames (id = stream id, value = bytes) as its application reads, about every quarter window. When a stream item uses up the producer's window it carries the WindowExhausted flag. If the consumer then runs out of data, it doubles its window, up to its maximum, and grants the increase. Bytes past the granted window are a protocol violation.

## Liveness

Each side sends a Ping every `HeartbeatInterval` and closes the connection after `IdleTimeout` without receiving anything. When a request times out and nothing has arrived since it was sent, the sender also sends a Ping with id `-2` and closes the connection if nothing arrives within `LivenessProbeTimeout`. Any frame counts as proof of life.

## Closing

A side that closes deliberately sends Close and then closes the socket. Pending requests on either side complete with `connection_closed`.
