package clusterpb

// Regenerate ioflux.pb.go and ioflux_grpc.pb.go from ioflux.proto. Requires buf
// plus the protoc-gen-go and protoc-gen-go-grpc plugins on PATH; see README.md.
// The generated code is committed, so building and testing IOFlux never needs
// buf or protoc.
//
//go:generate buf generate
