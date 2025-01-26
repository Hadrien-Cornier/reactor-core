# Deep Dive Outline: Project Reactor Programming Concepts

1. Introduction to Reactive Programming  
   1.1 What is Reactive?  
   1.2 Reactive Streams Specification  
   1.3 Key Benefits of Non-blocking I/O  

2. Core Reactor Types  
   2.1 Flux (0...N Items)  
   2.2 Mono (0...1 Item)  

3. Operators and Functional Style  
   3.1 Mapping, Filtering, Reducing  
   3.2 FlatMap and Asynchronous Composition  
   3.3 Error Handling (onErrorReturn, onErrorResume, retry)  

4. Concurrency Control  
   4.1 Schedulers and Threading  
   4.2 publishOn vs. subscribeOn  
   4.3 BoundedElastic, Parallel, Single  

5. Advanced Topics  
   5.1 Hot vs. Cold Publishers  
   5.2 Context Propagation  
   5.3 ConnectableFlux and Broadcasting  
   5.4 Testing with reactor-test  

# Core Concepts of Project Reactor

## Reactive Streams Specification

```java
// @pages If you're unfamiliar with the specification, know that it enforces a
// well-defined sequence of method calls and a backpressure strategy ensuring
// that Subscribers can request a manageable amount of data from Publishers.
// Pseudo-code for a Publisher
interface Publisher<T> {
void subscribe(Subscriber<? super T> subscriber);
}
// Pseudo-code for a Subscriber
interface Subscriber<T> {
void onSubscribe(Subscription s);
void onNext(T t);
void onError(Throwable t);
void onComplete();
}
// Pseudo-code for a Subscription
interface Subscription {
// Request n items from the Publisher
void request(long n);
// Cancel the subscription (stop receiving items)
void cancel();
}
// Pseudo-code for a Processor
interface Processor<T, R> extends Subscriber<T>, Publisher<R> {
// No additional methods, simply inherits both Subscriber and Publisher
}
```

### Basic Operation Flow

1. A <strong>Publisher</strong> has a "subscribe" method. When you call subscribe with a <strong>Subscriber</strong>, the Publisher creates or manages a <strong>Subscription</strong>, passing it to the <strong>Subscriber</strong>'s `onSubscribe(...)`.  
2. The <strong>Subscriber</strong> uses this Subscription to call `request(long n)`, signifying how many items it is ready to receive (backpressure).  
3. The <strong>Publisher</strong> starts pushing items via `onNext(T)`. Once all requested items are produced or an error/completion occurs, it signals `onComplete()` or `onError(Throwable)`.  

### Backpressure and the Order of Operator Calls

• <strong>Backpressure</strong> means aSubscriber controls how quickly data is emitted. Reactor aligns with the specification, providing operators like <code>limitRate</code> or <code>onBackpressureDrop</code> to handle overflows.  
• In a reactive chain, each operator is effectively a tiny <strong>Processor</strong>. For instance, <code>Flux.map</code> or <code>Flux.filter</code> subscribe to their upstream, apply transformation or filtering, and then publish to downstream.  
• When you chain operators, the flow typically proceeds from upstream to downstream. The act of requesting items starts from the final Subscriber, travels upstream, and triggers data production.  


---

These interfaces and their related specifications form the foundation of Project Reactor. Understanding the order of calls helps clarify why certain operators must be placed before or after others in a chain, how error-handling behaves, and where concurrency boundaries (`publishOn`, `subscribeOn`) are effectively inserted.

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


## Sync versus Async Publishers 

# Understanding Synchronous vs. Asynchronous Operators in Project Reactor

In Project Reactor, most operators are not inherently tied to their own concurrency or threading model. Rather, they compose (upstream) and transform (downstream) signals within the same thread context unless you introduce concurrency explicitly (for example, by using Schedulers or operators like flatMap that can merge asynchronous sources).

Below is a conceptual overview of which operators tend to be "synchronous" (operate in the same thread context) vs. "asynchronous" (potentially operate in different thread contexts or spawn new work):

---

## 1. Synchronous Operators

"Synchronous" in Project Reactor means that the operator processes data on the same thread on which it is invoked. It does not by itself introduce concurrency or switch threads.

• map:  
  Transforms each source element (e.g., String → Integer) in a one-to-one manner.  
  – It behaves synchronously: all transformations happen on the same thread that emits the original data.

• filter, take, reduce, etc.:  
  – Any operator that simply processes items inline—without dispatching to another Scheduler—works synchronously by default.

• publishOn or subscribeOn (unless they wrap an asynchronous source):  
  – They schedule subsequent stages onto a different thread, but the actual scheduling call is synchronous with respect to the flow of signals—though once moved to another thread, downstream execution is asynchronous from the caller's perspective.  
  – These operators introduce concurrency only in that they "hop" to another thread. The operator itself is not merging multiple asynchronous Streams.

---

## 2. Asynchronous Operators

Operators become "asynchronous" either when they themselves produce or merge asynchronous flows (e.g., multiple sources) or when they process data on another thread. Often, the prime example is flatMap.

• flatMap:  
  – "Flattens" a one-to-many or one-to-zero/one Publisher returned by a function.  
  – This allows you to trigger asynchronous operations (e.g., an HTTP call returning a Mono/Flux), merging results back into one output sequence.  
  – Conceptually, each source item spawns an inner subscription, which may run on a completely new thread.

• merge, zip, combineLatest, firstWithSignal, etc.:  
  – These can combine data from multiple concurrent Streams and interleave or coordinate them. They run asynchronously if the upstream Publishers are asynchronous or if they run on different Schedulers.

• delayElements, interval, repeatWhen, retryWhen, etc.:  
  – These operators often rely on a Scheduler (e.g., a timer thread) to schedule delays or repeated signals.

---

## 3. Why Is map Synchronous While flatMap Can Be Asynchronous?

• map:  
  – Expects a "pure" function: (T → R).  
  – It never internally subscribes to a separate Publisher; it just transforms the object and passes it along.  
  – Execution continues on the same thread that emits the original item.

• flatMap:  
  – Expects a function that returns a Publisher (e.g., Flux or Mono).  
  – Internally subscribes to each returned Publisher, which can run asynchronously (for instance, an HTTP call).  
  – Merges multiple concurrent inner streams back into a single sequence, often on multiple threads.

---

## 4. How Does This Work Under the Hood?

At a high level:

1. **Reactive Streams**:  
   – Reactor implements the Reactive Streams specification, where each Operator is a "Publisher" that emits signals (onNext, onError, onComplete) to its downstream "Subscriber."  
   – These signals flow synchronously by default, unless an operator specifically involves concurrency.

2. **Schedulers**:  
   – Operators like publishOn, subscribeOn, or those that rely on time (e.g., delayElements) employ Schedulers behind the scenes to shift work to a thread pool or timer.  
   – Otherwise, the pipeline runs in the same thread from upstream to downstream.

3. **Inner Publishers**:  
   – When flatMap is used, each emission from the source triggers a subscription to a new (inner) Publisher. If that Publisher is itself asynchronous (for instance, an HTTP Mono that uses a custom Scheduler), the overall flow continues asynchronously.

---

## Wrap-Up

• **Synchronous operators** (like map) do all their work in the thread they are called on and never spawn concurrency themselves.  
• **Asynchronous operators** (like flatMap) may merge multiple Publishers or schedule work on other threads, introducing concurrency or parallel processing in your stream.

Project Reactor leaves you in charge of if and when concurrency happens. By default, signals flow synchronously in the caller's thread. You add concurrency or scheduling explicitly with operators such as subscribeOn, publishOn, or via transforming with flatMap returning an async Publisher.

## Hot vs. Cold Publishers
- A cold publisher (the default for most Flux and Mono) starts emitting items when subscribed to. Each new Subscriber sees the entire sequence from the beginning.
- A hot publisher emits items regardless of whether anyone is subscribed; late subscribers may miss prior signals.

## Error Handling
Error signals end the sequence (similar to throwing an exception), but Reactor offers recovery operators:
- `onErrorReturn(...)` to provide a fallback value.
- `onErrorResume(...)` to switch to a fallback publisher.
- `retry(...)` to re-subscribe when encountering particular errors.

For advanced usage, combine `doOnError(...)` side effects with `onErrorMap(...)`, `onErrorContinue(...)`, etc.

## ConnectableFlux & ParallelFlux

### ConnectableFlux
A `ConnectableFlux` is a specialized form of `Flux` that allows you to control when the subscription to the source happens through the `connect()` method. This is particularly useful for:

1. **Hot Publishers**: Making a cold publisher hot
2. **Multicasting**: Sharing a single subscription among multiple subscribers
3. **Late Subscribers**: Managing late subscribers in a controlled way

Here's an example of ConnectableFlux usage:

```java
// Create a ConnectableFlux that emits every second
ConnectableFlux<Long> connectableFlux = Flux.interval(Duration.ofSeconds(1))
    .publish();

// First subscriber
connectableFlux.subscribe(i -> System.out.println("Subscriber 1: " + i));

// Second subscriber
connectableFlux.subscribe(i -> System.out.println("Subscriber 2: " + i));

// Nothing happens until we connect
connectableFlux.connect();
```

Common ConnectableFlux patterns:

```java
// Auto-connect when 2 subscribers are ready
Flux<Integer> flux = Flux.range(1, 3)
    .publish()
    .autoConnect(2);

// Ref-count: automatically connect when first subscriber arrives
// and cancel when last subscriber leaves
Flux<Integer> refCounted = Flux.range(1, 3)
    .publish()
    .refCount(1);

// Ref-count with grace period
Flux<Integer> refCountedGrace = Flux.range(1, 3)
    .publish()
    .refCount(1, Duration.ofSeconds(1));
```

### ParallelFlux vs flatMap with parallel()

While both `ParallelFlux` and `flatMap` with `parallel()` scheduler can achieve parallelization, they serve different purposes and have different characteristics:

#### ParallelFlux
```java
// Using ParallelFlux
ParallelFlux<Integer> parallelFlux = Flux.range(1, 10)
    .parallel(4)  // Split into 4 rails
    .runOn(Schedulers.parallel())
    .map(i -> performComputation(i));
```

#### FlatMap with parallel()
```java
// Using flatMap with parallel scheduler
Flux<Integer> flatMapParallel = Flux.range(1, 10)
    .flatMap(i -> Mono.just(i)
        .map(this::performComputation)
        .subscribeOn(Schedulers.parallel()),
        4  // concurrency hint
    );
```

Key Differences:

1. **Work Distribution**:
   - `ParallelFlux`: Evenly distributes work across a fixed number of rails (round-robin)
   - `flatMap`: Dynamically schedules work as it comes, which can lead to uneven distribution

2. **Ordering**:
   - `ParallelFlux`: No guaranteed ordering by default
   - `flatMap`: Can preserve ordering using `flatMapSequential` or `concatMap`

3. **Backpressure Handling**:
   - `ParallelFlux`: Maintains backpressure per rail
   - `flatMap`: Handles backpressure across all concurrent operations

4. **Use Cases**:
   - `ParallelFlux`: Better for CPU-bound tasks with predictable workloads
   - `flatMap`: Better for I/O-bound tasks or varying workloads

Example showing the differences:

```java
public class ParallelizationExample {
    
    public static void main(String[] args) {
        // ParallelFlux example
        Flux.range(1, 10)
            .parallel(2)
            .runOn(Schedulers.parallel())
            .map(i -> {
                System.out.println("ParallelFlux processing " + i + 
                    " on thread " + Thread.currentThread().getName());
                return i * 2;
            })
            .sequential() // merge back to regular Flux
            .subscribe();

        // flatMap with parallel example
        Flux.range(1, 10)
            .flatMap(i -> Mono.just(i)
                .map(val -> {
                    System.out.println("flatMap processing " + val + 
                        " on thread " + Thread.currentThread().getName());
                    return val * 2;
                })
                .subscribeOn(Schedulers.parallel()),
                2 // concurrency hint
            )
            .subscribe();
    }

    // Example showing ordering differences
    public static void demonstrateOrdering() {
        // ParallelFlux - order not guaranteed
        Flux.range(1, 5)
            .parallel()
            .runOn(Schedulers.parallel())
            .map(i -> i * 2)
            .sequential()
            .subscribe(i -> System.out.println("ParallelFlux: " + i));

        // flatMap with ordered variants
        Flux.range(1, 5)
            .flatMapSequential(i -> Mono.just(i)
                .map(val -> val * 2)
                .subscribeOn(Schedulers.parallel())
            )
            .subscribe(i -> System.out.println("Ordered flatMap: " + i));
    }

    // Example showing backpressure handling
    public static void demonstrateBackpressure() {
        // ParallelFlux with backpressure
        Flux.range(1, 100)
            .parallel(4)
            .runOn(Schedulers.parallel())
            .map(i -> heavyComputation(i))
            .sequential()
            .limitRate(10) // backpressure per rail
            .subscribe();

        // flatMap with backpressure
        Flux.range(1, 100)
            .flatMap(i -> Mono.just(i)
                .map(ParallelizationExample::heavyComputation)
                .subscribeOn(Schedulers.parallel()),
                10 // concurrency limit acts as backpressure
            )
            .subscribe();
    }

    private static int heavyComputation(int i) {
        // Simulate heavy computation
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return i * 2;
    }
}
```

### When to Use Which

1. Use `ParallelFlux` when:
   - You have CPU-intensive tasks
   - Work items take similar time to process
   - You want explicit control over the number of parallel rails
   - You need efficient backpressure handling per rail

2. Use `flatMap` with parallel scheduler when:
   - You have I/O-bound tasks
   - Work items have varying processing times
   - You need more flexibility in concurrency
   - Order preservation is important
   - You need dynamic concurrency adjustment

Both approaches can be combined with other reactive operators, but `ParallelFlux` has a more limited set of operators available while in parallel mode (before calling `sequential()`).

## Context Propagation in Project Reactor

### What is Context Propagation?

Context propagation is a mechanism that allows passing contextual information (like correlation IDs, security credentials, or transaction metadata) along the reactive pipeline without explicitly including it in the data flow. It's similar to ThreadLocal but works across asynchronous boundaries and different threads.

### When is it Useful?

Context propagation is particularly valuable in:

1. Distributed tracing
2. Security context propagation
3. Transaction management
4. MDC logging
5. Request scoping in web applications

### How it Works

Context in Project Reactor:
- Is immutable
- Flows from downstream to upstream (opposite to data flow)
- Is subscription-scoped
- Can be read using `deferContextual()` or `transformDeferredContextual()`
- Can be written using `contextWrite()`

### Code Examples

Here's a practical example demonstrating context propagation:

```java
public class ContextPropagationExample {
    private static final String CORRELATION_ID = "correlationId";
    private static final String USER_ID = "userId";

    public static void main(String[] args) {
        // Simulating a service call with context
        processRequest("GET /api/data", "user123")
            .contextWrite(Context.of(CORRELATION_ID, "req-123"))
            .subscribe(
                response -> System.out.println("Response: " + response),
                error -> System.err.println("Error: " + error)
            );
    }

    public static Mono<String> processRequest(String request, String userId) {
        return Mono.deferContextual(ctx -> {
            // Read from context
            String correlationId = ctx.getOrDefault(CORRELATION_ID, "unknown");
            
            return executeRequest(request)
                .transformDeferredContextual((mono, innerCtx) -> {
                    // Log with context information
                    System.out.println(String.format(
                        "Processing request [correlationId=%s, userId=%s]: %s",
                        correlationId,
                        innerCtx.getOrDefault(USER_ID, "anonymous"),
                        request
                    ));
                    return mono;
                })
                // Add more context information
                .contextWrite(Context.of(USER_ID, userId));
        });
    }

    private static Mono<String> executeRequest(String request) {
        return Mono.deferContextual(ctx -> {
            // Access context in business logic
            String correlationId = ctx.get(CORRELATION_ID);
            String userId = ctx.get(USER_ID);
            
            // Simulate API call
            return Mono.just(String.format(
                "Processed request for user %s with correlationId %s",
                userId,
                correlationId
            ));
        });
    }
}
```

### Advanced Context Usage with Hooks

Project Reactor 3.5.0 introduced enhanced context propagation support through hooks:

```java
public class ContextPropagationWithHooks {
    public static void main(String[] args) {
        // Enable automatic context propagation
        Hooks.enableAutomaticContextPropagation();

        // Create a ThreadLocal for demonstration
        ThreadLocal<String> threadLocal = new ThreadLocal<>();
        threadLocal.set("original-thread-value");

        Mono.defer(() -> {
            String value = threadLocal.get();
            return Mono.just("Processing with " + value);
        })
        .publishOn(Schedulers.boundedElastic())
        .doOnNext(result -> {
            // ThreadLocal value is preserved across thread boundaries
            System.out.println(result + " (ThreadLocal: " + threadLocal.get() + ")");
        })
        .subscribe();
    }
}
```

### Best Practices

1. **Immutability**: Treat context as immutable and use `contextWrite()` to create new contexts with additional values.

```java
// Good
mono.contextWrite(ctx -> ctx.put("key", "value"))

// Avoid modifying context directly
mono.contextWrite(ctx -> {
    ((Context) ctx).put("key", "value"); // Wrong!
    return ctx;
})
```

2. **Context Access**: Use `deferContextual()` for context-dependent operations.

```java
// Good
Mono.deferContextual(ctx -> 
    Mono.just("Value for " + ctx.get("key"))
)

// Avoid storing context in variables
Context globalCtx; // Wrong!
```

3. **Error Handling**: Always provide default values when reading from context.

```java
// Good
ctx.getOrDefault("key", "defaultValue")

// Instead of
try {
    ctx.get("key")
} catch (Exception e) {
    // Handle missing key
}
```

Context propagation is particularly useful in microservices architectures where you need to maintain contextual information across service boundaries and asynchronous operations. It provides a clean way to pass metadata without polluting your business logic or method signatures.

## ConnectableFlux & ParallelFlux
- `ConnectableFlux` allows multiple subscribers to observe the same data source together, but the source only starts producing once "connected."
- `ParallelFlux` splits a Flux across multiple "rails" (threads) for parallel processing. You typically merge or reduce these rails back into one Flux when you are finished parallelizing.

These core objects and features form the backbone of Reactor. By chaining operators on Flux or Mono, controlling concurrency with Schedulers, handling errors proactively, and optionally sharing or splitting data streams with specialized types, you can build expressive, reactive applications that handle data in a non-blocking, event-driven manner.

# Appendix: 

### Additional Threading Explanation

In many Java applications, you may observe additional threads running even before your Reactive pipeline starts operating (for example, before a flatMap operator is invoked). Common threads you might see include:

• main – The primary application thread that launches your code.  
• Finalizer – A JVM-managed thread that finalizes objects whose finalizers have not yet been invoked.  
• ReferenceHandler – A JVM-managed thread that handles reference queues (e.g., for SoftReferences, WeakReferences).  
• SignalDispatcher – A low-level JVM thread that processes operating system signals (e.g., signals for handling Ctrl+C interrupts on some systems).  

These threads are part of the basic Java Virtual Machine mechanism and are not introduced by Reactor or any other concurrency library. They are generally used for internal bookkeeping, garbage collection, and system-level signal handling. Consequently, even if your code is configured to run on a single thread from a Reactor perspective, the JVM itself can still have multiple system threads active alongside your application logic.
