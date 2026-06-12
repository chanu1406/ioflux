// Package payload generates replay payload bytes.
package payload

import (
	"encoding/binary"
	"hash/fnv"
)

// Mode selects how write and materialization buffers are filled.
type Mode string

const (
	ModeSeeded Mode = "seeded"
	ModeZero   Mode = "zero"
)

// DefaultSeed is recorded in run metadata when callers do not choose a seed.
const DefaultSeed int64 = 1

// Config controls payload generation.
type Config struct {
	Mode Mode
	Seed int64
}

// Normalize fills unset fields with production defaults.
func (c Config) Normalize() Config {
	if c.Mode == "" {
		c.Mode = ModeSeeded
	}
	if c.Seed == 0 {
		c.Seed = DefaultSeed
	}
	return c
}

// Fill writes deterministic bytes into dst. Seeded mode is stable across chunk
// boundaries: the same (seed, opID, target, absolute offset) always yields the
// same bytes regardless of how callers slice the buffer. The byte at absolute
// position a is byte (a mod 8) of the little-endian encoding of
// splitmix64(base + (a/8)·golden); Fill generates one word per 8-byte block
// rather than recomputing it per byte, since materialization pushes GiBs
// through this path.
func Fill(dst []byte, cfg Config, opID int64, target string, off int64) {
	cfg = cfg.Normalize()
	if cfg.Mode == ModeZero {
		clear(dst)
		return
	}
	targetHash := hashString64(target)
	base := uint64(cfg.Seed) ^
		rotateLeft(uint64(opID)*0x9e3779b97f4a7c15, 17) ^
		rotateLeft(targetHash, 31)

	wordAt := func(abs int64) uint64 {
		return splitmix64(base + uint64(abs>>3)*0x9e3779b97f4a7c15)
	}

	i := 0
	abs := off
	// Partial leading block when off is not 8-aligned.
	if rem := int(abs & 7); rem != 0 && i < len(dst) {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], wordAt(abs))
		n := copy(dst, tmp[rem:])
		i += n
		abs += int64(n)
	}
	// Full 8-byte blocks.
	for ; i+8 <= len(dst); i, abs = i+8, abs+8 {
		binary.LittleEndian.PutUint64(dst[i:i+8], wordAt(abs))
	}
	// Partial trailing block.
	if i < len(dst) {
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], wordAt(abs))
		copy(dst[i:], tmp[:len(dst)-i])
	}
}

func hashString64(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	return h.Sum64()
}

func rotateLeft(x uint64, k uint) uint64 {
	return (x << k) | (x >> (64 - k))
}

func splitmix64(x uint64) uint64 {
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}
