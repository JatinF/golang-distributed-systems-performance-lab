// Package reason holds the lab outcome vocabulary.
package reason

const (
	Done                 = "done"
	Duplicate            = "duplicate"
	Backpressure         = "backpressure"
	Cancelled            = "cancelled"
	DeadlineExceeded     = "deadline_exceeded"
	ShuttingDown         = "shutting_down"
	CacheHit             = "cache_hit"
	CacheMiss            = "cache_miss"
	CacheInvalidated     = "cache_invalidated"
	CacheUnavailable     = "cache_unavailable"
	CacheRecovered       = "cache_recovered"
	Lost                 = "lost"
	Retried              = "retried"
	Replayed             = "replayed"
	TokenExpired         = "token_expired"
	TokenInvalid         = "token_invalid"
	SignatureInvalid     = "signature_invalid"
	AudienceMismatch     = "audience_mismatch"
	Unauthenticated      = "unauthenticated"
	StoreUnavailable     = "store_unavailable"
	MessagingUnavailable = "messaging_unavailable"
	Rejected             = "rejected"
	NotStarted           = "not-started"
)
