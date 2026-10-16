# Focused GoBard Gateway lifecycle patch

Based on ozraru/discordgo 5cce117199f7. OpenContext applies caller cancellation,
a 30-second opening budget, and the Session lifetime. Shutdown is terminal:
close worker admission, cancel opening/reconnect, close the WebSocket and join
Gateway workers and dispatched callbacks. Repeated shutdown joins the same
operation. Call it from the owner, never an event callback. Close remains a
nonjoining socket disconnect suitable for worker use; it no longer sleeps as a
substitute for joining. Automatic reconnect has a three-attempt finite budget.

Callbacks must return cooperatively. This patch does not own caller-spawned
goroutines or the SDK's legacy voice connections. GoBard uses SyncEvents=true,
ShouldReconnectOnError=false, bounded synchronous callbacks/admission and a
separate voice SDK. The general asynchronous event API retains its previous
admission behavior; it is now joined but this patch does not add a new event
queue/backpressure policy for unrelated SDK consumers.

Verified 2026-10-16: `go test -race ./...`, go vet, and disposable GoBard integration
race tests. Local WebSocket fixtures cover stalled Hello cancellation, terminal
admission, repeated shutdown and an accepted callback that must finish before
shutdown returns. No real Gateway reconnection/listening acceptance is claimed.
