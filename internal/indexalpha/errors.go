package indexalpha

import "errors"

// Structured failure classes for the Index Alpha adapter. Each is wrapped with
// the status and the vendor's detail so the message stays actionable while
// errors.Is still classifies it at the MCP boundary.
var (
	// ErrUnauthorized means the Bearer token was missing, inactive, expired, or
	// bound to an inactive subscription. Retrying the same key will not help.
	ErrUnauthorized = errors.New("indexalpha: unauthorized")

	// ErrMalformedPayload means the response did not honour the documented
	// contract: not JSON, success=false, a shape other than the row array the
	// endpoint promises, or rows that violate the source invariants (shares not
	// in whole lots, negative values, duplicate brokers, Σbuy ≠ Σsell).
	ErrMalformedPayload = errors.New("indexalpha: malformed payload")

	// ErrUpstream is any other non-2xx status (400/422 request rejections,
	// 5xx). It is distinct from the classified failures above so a caller can
	// tell "the vendor said no" from "we could not talk to the vendor".
	ErrUpstream = errors.New("indexalpha: upstream error")

	// ErrInvalidRequest means the caller asked for something the contract
	// forbids before any HTTP call was made: an empty batch, or one over the
	// 50-ticker limit. It is a programming error, never a vendor condition.
	ErrInvalidRequest = errors.New("indexalpha: invalid request")
)
