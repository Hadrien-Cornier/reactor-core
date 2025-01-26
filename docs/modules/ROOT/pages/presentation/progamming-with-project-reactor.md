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

Below is a conceptual overview of which operators tend to be “synchronous” (operate in the same thread context) vs. “asynchronous” (potentially operate in different thread contexts or spawn new work):

---

## 1. Synchronous Operators

“Synchronous” in Project Reactor means that the operator processes data on the same thread on which it is invoked. It does not by itself introduce concurrency or switch threads.

• map:  
  Transforms each source element (e.g., String → Integer) in a one-to-one manner.  
  – It behaves synchronously: all transformations happen on the same thread that emits the original data.

• filter, take, reduce, etc.:  
  – Any operator that simply processes items inline—without dispatching to another Scheduler—works synchronously by default.

• publishOn or subscribeOn (unless they wrap an asynchronous source):  
  – They schedule subsequent stages onto a different thread, but the actual scheduling call is synchronous with respect to the flow of signals—though once moved to another thread, downstream execution is asynchronous from the caller’s perspective.  
  – These operators introduce concurrency only in that they “hop” to another thread. The operator itself is not merging multiple asynchronous Streams.

---

## 2. Asynchronous Operators

Operators become “asynchronous” either when they themselves produce or merge asynchronous flows (e.g., multiple sources) or when they process data on another thread. Often, the prime example is flatMap.

• flatMap:  
  – “Flattens” a one-to-many or one-to-zero/one Publisher returned by a function.  
  – This allows you to trigger asynchronous operations (e.g., an HTTP call returning a Mono/Flux), merging results back into one output sequence.  
  – Conceptually, each source item spawns an inner subscription, which may run on a completely new thread.

• merge, zip, combineLatest, firstWithSignal, etc.:  
  – These can combine data from multiple concurrent Streams and interleave or coordinate them. They run asynchronously if the upstream Publishers are asynchronous or if they run on different Schedulers.

• delayElements, interval, repeatWhen, retryWhen, etc.:  
  – These operators often rely on a Scheduler (e.g., a timer thread) to schedule delays or repeated signals.

---

## 3. Why Is map Synchronous While flatMap Can Be Asynchronous?

• map:  
  – Expects a “pure” function: (T → R).  
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
   – Reactor implements the Reactive Streams specification, where each Operator is a “Publisher” that emits signals (onNext, onError, onComplete) to its downstream “Subscriber.”  
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

Project Reactor leaves you in charge of if and when concurrency happens. By default, signals flow synchronously in the caller’s thread. You add concurrency or scheduling explicitly with operators such as subscribeOn, publishOn, or via transforming with flatMap returning an async Publisher.

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
- `ConnectableFlux` allows multiple subscribers to observe the same data source together, but the source only starts producing once "connected."
- `ParallelFlux` splits a Flux across multiple "rails" (threads) for parallel processing. You typically merge or reduce these rails back into one Flux when you are finished parallelizing.

## Context Propagation
Reactor includes support for a lightweight contextual data store known as Context.
Operators like `contextWrite(...)` and `deferContextual(...)` let you attach and retrieve values, useful for carrying around data such as correlation IDs or user info without resorting to ThreadLocal (especially crucial in highly concurrent or reactive environments).

These core objects and features form the backbone of Reactor. By chaining operators on Flux or Mono, controlling concurrency with Schedulers, handling errors proactively, and optionally sharing or splitting data streams with specialized types, you can build expressive, reactive applications that handle data in a non-blocking, event-driven manner.

# Appendix: 

### Additional Threading Explanation

In many Java applications, you may observe additional threads running even before your Reactive pipeline starts operating (for example, before a flatMap operator is invoked). Common threads you might see include:

• main – The primary application thread that launches your code.  
• Finalizer – A JVM-managed thread that finalizes objects whose finalizers have not yet been invoked.  
• ReferenceHandler – A JVM-managed thread that handles reference queues (e.g., for SoftReferences, WeakReferences).  
• SignalDispatcher – A low-level JVM thread that processes operating system signals (e.g., signals for handling Ctrl+C interrupts on some systems).  

These threads are part of the basic Java Virtual Machine mechanism and are not introduced by Reactor or any other concurrency library. They are generally used for internal bookkeeping, garbage collection, and system-level signal handling. Consequently, even if your code is configured to run on a single thread from a Reactor perspective, the JVM itself can still have multiple system threads active alongside your application logic.
