package trainingread

import (
	"math"
	"math/rand/v2"
)

// lognormalSampler draws record sizes from a lognormal distribution.
//
// The distribution is parameterized by the desired mean and a fixed log-space
// standard deviation (sigma=0.5), giving a coefficient of variation of ~0.53 —
// a plausible spread for shard records in compressed datasets.
//
// For lognormal X: E[X] = exp(mu + sigma^2/2). Setting this to the desired
// mean M gives mu = ln(M) - sigma^2/2.
type lognormalSampler struct {
	rng   *rand.Rand
	mu    float64
	sigma float64
}

const lognormalSigma = 0.5

func newLognormalSampler(rng *rand.Rand, mean int64) *lognormalSampler {
	mu := math.Log(float64(mean)) - lognormalSigma*lognormalSigma/2
	return &lognormalSampler{rng: rng, mu: mu, sigma: lognormalSigma}
}

// Sample returns a positive integer drawn from the lognormal distribution.
// The minimum return value is 1.
func (s *lognormalSampler) Sample() int64 {
	z := s.rng.NormFloat64()
	v := math.Round(math.Exp(s.mu + s.sigma*z))
	if v < 1 {
		return 1
	}
	return int64(v)
}
