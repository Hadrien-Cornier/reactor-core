# Project Reactor Programming Guide

## Core Concepts

### Operation Flow
1. Publisher.subscribe(Subscriber) → creates Subscription
2. Subscriber.request(n) → signals demand (backpressure)
3. Publisher.onNext()/onComplete()/onError() → emits data

### Backpressure
- Subscriber controls emission rate via `request(n)` calls
- Handled via `limitRate`, `onBackpressureDrop`
- Each operator acts as Processor: upstream → transform → downstream
- Request flow: downstream → upstream

### Reactive Streams
```java
Publisher<T>     -> emit data
Subscriber<T>    -> consume data
Subscription     -> control demand (backpressure)
Processor<T,R>   -> transform data
```

### Mono & Flux

```java
// Mono: 0-1 items
Mono<T>   // async, lazy, single-value

// Flux: 0-N items
Flux<T>   // async, lazy, stream
```

```java
// Common Creation Methods
Mono.just(value)
Mono.fromCallable(() -> blockingCall())
Mono.defer(() -> dynamicMono())
Mono.empty()
Mono.error(ex)

Flux.just(1,2,3)
Flux.fromIterable(list)
Flux.range(1,10)
Flux.interval(Duration.ofSeconds(1))
```

## Sync vs Async Operators

### Synchronous Operators
- Execute in same thread
- No concurrency introduced
```java
// map: pure transform (T → R)
flux.map(i -> i * 2)
    .filter(i -> i > 5)
    .reduce(0, Integer::sum)
```

Common sync operators:
- `map`: one-to-one transform
- `filter`, `take`, `reduce`: inline processing
- `publishOn`/`subscribeOn`: scheduling call only

### Asynchronous Operators
- May execute across threads
- Introduce concurrency
```java
// flatMap: async operations
flux.flatMap(i -> webClient.get()
        .uri("/api/{i}", i)
        .retrieve()
        .bodyToMono(Response.class))
    .mergeWith(otherFlux)
    .delayElements(Duration.ofMillis(100))
```

Common async operators:
- `flatMap`: async transformations
- `merge`/`zip`: combine streams
- `delay`/`interval`: timer operations

### Error Handling
```java
flux.onErrorReturn(fallback)      // Return value
    .onErrorResume(ex -> backup)  // Switch publisher
    .onErrorContinue((ex,obj) -> 
         log.error("Skipping: {}", obj)) // Continue after error
    .retry(3)                     // Retry n times
```

### Threading
```java
// Default: subscriber's thread
flux.subscribe()

// Change thread context
flux.publishOn(scheduler)    // affects downstream
flux.subscribeOn(scheduler)  // affects whole chain

// Built-in schedulers
boundedElastic() // I/O operations (default: cpu*10 threads)
parallel()       // CPU-intensive (default: cpu threads)
single()         // Sequential operations
immediate()      // Current thread
fromExecutorService(executor) // Custom thread pool
```


### Detailed Scheduler Types

```java
// 1. boundedElastic() - Best for I/O
// Characteristics:
// - Creates worker pools as needed
// - Caps threads (CPU cores × 10)
// - Queues excess tasks
// - 60s idle timeout
// - Use for: I/O, blocking, JDBC
Flux.fromIterable(files)
    .flatMap(file -> Mono.fromCallable(() -> readFile(file))
        .subscribeOn(Schedulers.boundedElastic()))

// 2. parallel() - Best for CPU work
// Characteristics:
// - Fixed pool (CPU cores)
// - Never releases threads
// - Use for: computation, math
Flux.range(1, 100)
    .parallel()
    .runOn(Schedulers.parallel())
    .map(this::computeIntensive)
    .sequential()

// 3. single() - Best for ordering
// Characteristics:
// - Single worker thread
// - Reused across calls
// - Use for: sequential ops
Flux.range(1, 100)
    .publishOn(Schedulers.single())
    .map(i -> performSequentialOperation(i))

// 4. immediate() - No threading
// Characteristics:
// - Uses caller's thread
// - No thread overhead
// - Use for: testing, debug
Flux.range(1, 100)
    .publishOn(Schedulers.immediate())
    .map(i -> i * 2)
```

### Async Operations (flatMap)
```java
// Simple async example
userIds.flatMap(id -> 
    // Async HTTP call returning Mono<User>
    webClient.get()
             .uri("/users/{id}", id)
             .retrieve()
             .bodyToMono(User.class)
).subscribe()
```

### Hot vs Cold Publishers
```java
// Cold (default) - per-subscriber sequence
Flux<Integer> cold = Flux.range(1,3);

// Hot - shared sequence
DirectProcessor<Integer> hot = DirectProcessor.create();
hot.onNext(1); // Emitted regardless of subscribers

// Convert cold to hot
Flux<Integer> shared = cold.share(); // Uses multicast
```

### Context Propagation (Downstream -> Upstream)
```java
// Context flows opposite to data
Mono.deferContextual(ctx -> 
    Mono.just("User:" + ctx.get("user"))
)
.contextWrite(Context.of("user", "john"))
.subscribe(System.out::println); // "User:john"
```

## 2. Design Patterns & Best Practices

### Common Reactive Patterns

```java
// Circuit Breaker Pattern
Mono<Response> withCircuitBreaker(Request req) {
    return Mono.just(req)
        .transform(CircuitBreaker.of("service")
            .run())
        .timeout(Duration.ofSeconds(1))
        .retryWhen(Retry.backoff(3, Duration.ofMillis(100)));
}

// Bulkhead Pattern (Resource Isolation)
class BulkheadExample {
    private final Scheduler dedicated = 
        Schedulers.newBoundedElastic(10, 100, "service-a");
    
    Mono<Response> execute(Request req) {
        return Mono.just(req)
            .subscribeOn(dedicated)
            .map(this::process);
    }
}

// Cache Pattern
class CacheExample {
    private final LoadingCache<String, Mono<Data>> cache = 
        Caffeine.newBuilder()
            .buildAsync(key -> fetchData(key).cache());
    
    Mono<Data> getData(String key) {
        return Mono.fromFuture(cache.get(key));
    }
}

// Saga Pattern (Distributed Transactions)
Mono<OrderResult> orderSaga(Order order) {
    return validateOrder(order)
        .flatMap(this::reserveInventory)
        .flatMap(this::processPayment)
        .flatMap(this::shipOrder)
        .doOnError(this::compensate);
}
```

### Performance Optimization

```java
// Batching & Buffering
Flux<List<Event>> batchEvents(Flux<Event> source) {
    return source
        .bufferTimeout(100, Duration.ofMillis(50))  // Size or time
        .onBackpressureBuffer(10_000, BufferOverflowStrategy.DROP_OLDEST);
}

// Prefetch Tuning
Flux<Data> optimizedPrefetch(Flux<Request> reqs) {
    return reqs.flatMap(
        r -> process(r),
        maxConcurrent = 8,    // Parallel processes
        prefetch = 32         // Items pre-fetched
    );
}

// Memory Management
class ResourceManagement {
    Flux<Data> withCleanup(Resource resource) {
        return Flux.using(
            () -> resource,           // Acquire
            r -> process(r),          // Use
            Resource::close,          // Cleanup
            true                      // Eager cleanup
        );
    }
}
```

### Testing Strategies

```java
// Unit Testing with StepVerifier
@Test
void testReactiveFlow() {
    Flux<String> source = service.getData();
    
    StepVerifier.create(source)
        .expectNext("a", "b")
        .expectComplete()
        .verify(Duration.ofSeconds(5));
}

// Virtual Time Testing
@Test
void testTimeBasedOps() {
    StepVerifier.withVirtualTime(() -> 
        Flux.interval(Duration.ofHours(1)).take(2)
    )
    .expectSubscription()
    .expectNoEvent(Duration.ofHours(1))
    .expectNext(0L)
    .thenAwait(Duration.ofHours(1))
    .expectNext(1L)
    .verifyComplete();
}

// Test Publishers
@Test
void testWithTestPublisher() {
    TestPublisher<String> publisher = TestPublisher.create();
    Flux<String> flux = publisher.flux().map(String::toUpperCase);
    
    StepVerifier.create(flux)
        .then(() -> publisher.emit("a", "b"))
        .expectNext("A", "B")
        .verifyComplete();
}
```

### Common Anti-patterns & Pitfalls

```java
// DON'T: Block in reactive chain
Flux.just(1,2,3)
    .map(i -> {
        Thread.sleep(100);    // WRONG!
        return process(i);
    })

// DO: Use proper async wrapper
Flux.just(1,2,3)
    .flatMap(i -> Mono
        .fromCallable(() -> {
            Thread.sleep(100);
            return process(i);
        })
        .subscribeOn(Schedulers.boundedElastic())
    )

// DON'T: Unbounded concurrency
source.flatMap(item -> 
    service.process(item), 
    Integer.MAX_VALUE        // WRONG!
)

// DO: Limit concurrency
source.flatMap(item ->
    service.process(item),
    maxConcurrent = 10      // RIGHT!
)

// DON'T: Ignore errors
flux.subscribe(
    data -> process(data)   // WRONG: No error handler
)

// DO: Handle errors
flux.subscribe(
    data -> process(data),
    error -> handleError(error)
)
```

### Best Practices

```java
// 1. Scheduler Selection
class SchedulerUsage {
    // GOOD: I/O operations
    Mono<Data> ioOperation() {
        return Mono.fromCallable(() -> readFile())
            .subscribeOn(Schedulers.boundedElastic());
    }
    
    // GOOD: CPU-intensive tasks
    Flux<Data> computeIntensive() {
        return Flux.range(1, 1000)
            .parallel()
            .runOn(Schedulers.parallel())
            .map(this::heavyComputation)
            .sequential();
    }
}

// 2. Custom Thread Pools
class CustomThreadPools {
    private final Scheduler customPool = Schedulers.newBoundedElastic(
        Runtime.getRuntime().availableProcessors() * 2,  // threads
        1000,                                           // queue size
        "api-pool"
    );
    
    Mono<Response> apiCall() {
        return Mono.fromCallable(() -> externalCall())
            .subscribeOn(customPool)
            .doFinally(signal -> {
                if (isShuttingDown) customPool.dispose();
            });
    }
}

// 3. Metrics & Monitoring
class MetricsExample {
    Mono<Data> trackedOperation(String opName) {
        return Mono.fromCallable(() -> operation())
            .name(opName)
            .tag("type", "business")
            .metrics()
            .transformDeferred(mono -> 
                mono.doOnSubscribe(s -> recordStart(opName))
                   .doFinally(s -> recordEnd(opName))
            );
    }
}
```


## Next Steps
1. Practice with [Project Reactor Test](https://github.com/reactor/reactor-core/tree/main/reactor-test)
2. Explore [Reactor Debugging Guide](https://projectreactor.io/docs/core/release/reference/#debugging)
3. Join [Reactor Gitter](https://gitter.im/reactor/reactor)


## APPENDIX

# Core Concepts of Project Reactor

## Mono and Flux Publishers
- `Mono<T>` represents a lazy, potentially asynchronous computation that can emit at most one item (0 or 1) and then complete (or error).
- `Flux<T>` represents a lazy, potentially asynchronous emission of 0 to N items. After the final element, it either completes or terminates with an error.

You typically interact with values through operators such as `map`, `flatMap`, `filter`, or `reduce`. The processing does not start until you call `subscribe()`.

## Schedulers and Threading
By default, work executes in whichever thread calls `subscribe()`, but you can use:
- `publishOn(scheduler)` to switch execution contexts starting from that point in the chain (affects ensuing operators).
- `subscribeOn(scheduler)` to switch execution contexts at subscription time (affects the whole chain, starting at the source).

Built-in schedulers include:
- `Schedulers.boundedElastic()` for blocking I/O work.
- `Schedulers.parallel()` for CPU-intensive tasks.

You can also create your own via `Schedulers.newBoundedElastic(...)` or `Schedulers.fromExecutor(...)`.


## Hot vs. Cold Publishers
- A cold publisher (the default for most Flux and Mono) starts emitting items when subscribed to. Each new Subscriber sees the entire sequence from the beginning.
- A hot publisher emits items regardless of whether anyone is subscribed; late subscribers may miss prior signals.

## Error Handling
Error signals end the sequence (similar to throwing an exception), but Reactor offers recovery operators:
- `onErrorReturn(...)` to provide a fallback value.
- `onErrorResume(...)` to switch to a fallback publisher.
- `retry(...)` to re-subscribe when encountering particular errors.

For advanced usage, combine `doOnError(...)` side effects with `onErrorMap(...)`, `onErrorContinue(...)`, etc.

## ConnectableFlux

Controls subscription timing for multiple subscribers. Used to:
- Convert cold → hot publishers
- Share subscriptions
- Manage late subscribers

### Basic Usage
```java
// Manual control
ConnectableFlux<Long> flux = Flux.interval(Duration.ofSeconds(1))
    .publish();
flux.subscribe(i -> log.info("Sub1: {}", i));
flux.subscribe(i -> log.info("Sub2: {}", i));
flux.connect();  // Starts emission
```

### Common Patterns
```java
Flux.range(1, 3)
    .publish()
    .autoConnect(2);     // Start with N subscribers
    .refCount(1);        // Start with first sub, stop when last leaves
    .refCount(1, Duration.ofSeconds(1));  // Grace period before stopping
```

## Context Propagation in Project Reactor

### What is Context Propagation?

ThreadLocal-like functionality for reactive streams, preserving context across async boundaries.

### Use Cases
- Distributed tracing
- Security context
- Transaction metadata
- MDC logging
- Request scoping

### Core Concepts
- Immutable
- Flows downstream → upstream
- Subscription-scoped
- Read: `deferContextual()`
- Write: `contextWrite()`

### Basic Usage
```java
// Write context
Mono<Response> withContext = service.process()
    .contextWrite(Context.of("userId", "123"));

// Read context
Mono<String> readContext = Mono.deferContextual(ctx ->
    Mono.just("User: " + ctx.getOrDefault("userId", "anonymous"))
);

// Combine read/write
Mono<String> process = Mono.deferContextual(ctx -> {
    String userId = ctx.get("userId");
    return callService(userId)
        .contextWrite(Context.of("traceId", "xyz"));
});
```

### Automatic Propagation (3.5.0+)
```java
// Enable ThreadLocal propagation
Hooks.enableAutomaticContextPropagation();

ThreadLocal<String> threadLocal = new ThreadLocal<>();
threadLocal.set("value");

Mono.defer(() -> Mono.just(threadLocal.get()))
    .publishOn(Schedulers.boundedElastic())
    .subscribe(); // ThreadLocal value preserved
```

### Context : Do and Don't
```java
// DO: Immutable updates
mono.contextWrite(ctx -> ctx.put("key", "value"))

// DO: Default values
ctx.getOrDefault("key", "default")

// DON'T: Store context
Context globalCtx; // Wrong!

// DON'T: Modify directly
ctx.put("key", "value"); // Wrong!
```

Context propagation is particularly useful in microservices architectures where you need to maintain contextual information across service boundaries and asynchronous operations. It provides a clean way to pass metadata without polluting your business logic or method signatures.

