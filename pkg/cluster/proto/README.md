# clusterpb — coordinator/worker wire protocol

`ioflux.proto` defines the gRPC `Worker` service used to distribute a replay run
across hosts. Workers are gRPC servers (`ioflux worker --listen`); the
coordinator (`ioflux run --hosts ...`) is the client.

The generated Go files (`ioflux.pb.go`, `ioflux_grpc.pb.go`) are **committed**, so
building, testing, and CI never require `buf` or `protoc`.

## Regenerating

Only needed when `ioflux.proto` changes. Install the toolchain once:

```bash
go install github.com/bufbuild/buf/cmd/buf@latest
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

Then regenerate (the plugins must be on `PATH`, e.g. `$(go env GOPATH)/bin`):

```bash
cd pkg/cluster/proto
buf generate          # or: go generate ./...
```

`buf` compiles the `.proto` directly (no separate `protoc` binary needed) and
writes the `*.pb.go` files next to the source via `paths=source_relative`.
