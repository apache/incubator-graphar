# GraphAr Format Specification

This folder contains protocol definitions for the GraphAr format.

## How to generate code

### Prerequisites

- [protoc](https://developers.google.com/protocol-buffers/docs/downloads)
- [buf](https://buf.build/docs/installation) (version >= 1.32.0)

### Build

the build process is managed by `buf` and runs in the root of the repository.

```bash
buf generate
```

For documentation about the format, see the [GraphAr documentation](https://graphar.apache.org/docs/specification/format).
