package middleware

import (
	"net/http"
	"os"

	"go.uber.org/zap"
)

// A state critical route is an HTTP handler that manages essential state
// needed for the system to function correctly. If a panic occurs while
// handling a request to such a route, the process will immediately
// terminate. This is necessary because panics in these routes usually
// indicate a violation of core invariants (for example, if the internal
// data structures for workers, heartbeats, and shard counts become
// inconsistent). When these invariants are broken, it is unsafe to
// continue running, so the process must exit to avoid further issues.
func StateCriticalRoute(h http.HandlerFunc, logger *zap.Logger) http.HandlerFunc {
	return func(res http.ResponseWriter, req *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("state critical route paniced", zap.Any("err", err))
				os.Exit(1)
			}
		}()
		h(res, req)
	}
}
