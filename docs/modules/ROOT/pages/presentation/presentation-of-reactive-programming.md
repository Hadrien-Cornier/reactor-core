# Introduction to Reactive Programming with Project Reactor

This document provides an introductory overview of reactive programming as seen through Project Reactor and Spring. We explore how reactive programming addresses the challenges of handling many concurrent requests and discuss high-level concepts, from the Reactive Manifesto to the internal event-loop model that drives concurrency in a Spring-based application.

---

## 1. The Reactive Manifesto in Brief

• The Reactive Manifesto outlines principles for modern systems: Responsive, Resilient, Elastic, and Message-Driven.  
• “Responsive” means reacting to users in a timely manner.  
• “Resilient” systems stay operational under failure.  
• “Elastic” systems scale up or down to handle variable loads.  
• “Message-Driven” designs rely on asynchronous message passing to avoid blocking.

These guiding principles help us build software that handles concurrency without overwhelming resources.

---

## 2. Historical Context: Thread-Per-Request Model

In the past, Java web servers handled each request with a dedicated OS thread. For small numbers of concurrent requests, this was fine. But large traffic:
• Consumed huge memory (thread stacks)  
• Caused high context-switching overhead in the OS  
• Led to thread starvation under load  

Although simple to conceptualize, the thread-per-request model struggles with systems requiring high concurrency.

---

## 3. Introduction to Reactive Programming

Reactive programming is a paradigm that:
• Builds on asynchronous, non-blocking I/O  
• Delivers data and signals (e.g., completion or errors) through **Publishers**  
• Allows consumers, known as **Subscribers**, to process this data on an event-driven basis  
• Encourages a declarative style of composing transformations  

Two popular Java libraries are:
• **Project Reactor** (part of the Spring ecosystem)  
• **RxJava** (inspired by Reactive Extensions from Microsoft)

Both implement the Reactive Streams specification. Reactor is the library we’ll focus on.

---

## 4. Basic Reactive Interfaces

Reactive Streams defines four main interfaces:

1. **Publisher<T>**  
   Emits a sequence of items (or signals) of type T to its **Subscriber**.
   
2. **Subscriber<T>**  
   Listens for items from the Publisher. Methods:  
   • onSubscribe(Subscription s)  
   • onNext(T item)  
   • onError(Throwable t)  
   • onComplete()

3. **Subscription**  
   Controls the relationship between Publisher and Subscriber.  
   Allows requesting items or canceling the flow.

4. **Processor<T,R>**  
   A hybrid of Publisher & Subscriber. Consumes and emits data, often used as a bridge.

Operators (e.g., `map`, `flatMap` in Reactor) build on these concepts. A chain of operators transforms the data as events flow from Publisher to Subscriber.

---

## 5. Advantages of Project Reactor

Compared to traditional blocking approaches:
• Optimized resource usage by avoiding idle threads during blocking I/O  
• Scales to large numbers of concurrent connections  
• Minimizes overhead from context switching  
• Integrates deeply with Spring WebFlux and other frameworks  

Disadvantages:
• Debugging can be trickier due to async flows  
• Familiar synchronous paradigms (thread locals, blocking) need adaptation  
• Requires a mindset shift for developers

---

## 6. Simplified Example Code

Below is a small snippet illustrating a typical Reactor flow:

```java
import reactor.core.publisher.Flux;
public class ReactorExample {
public static void main(String[] args) {
    Flux<String> names = Flux.just("Alice", "Bob", "Charlie")
    .map(name -> name.toUpperCase())
    .filter(name -> name.startsWith("C"));
    names.subscribe(
    value -> System.out.println("Received: " + value),
    error -> System.err.println("Error: " + error),
    () -> System.out.println("Completed.")
    );
    }
}
```


What happens:
1. `Flux.just(...)` is the Publisher that emits three names.  
2. `map(...)` transforms each string to uppercase.  
3. `filter(...)` allows only names starting with “C.”  
4. `subscribe(...)` is the Subscriber consuming the data.

---

## 7. Event Loops in Reactive Systems

### 7.1 The Role of the Event Loop

• In many reactive frameworks, a small pool of threads (often called event loops) handle I/O readiness events.  
• When new data arrives or a write becomes possible, the OS notifies the library through a callback.  
• The library schedules the logic in that callback, possibly passing it to an available thread from the event loop or a specialized scheduler.

### 7.2 Spring WebFlux and Reactor

Spring WebFlux uses the Reactor Netty runtime by default. Each Netty event loop thread:
• Waits for read/write readiness from the OS  
• Dispatches the request to the Reactor pipeline for processing  
• Switches rapidly among requests without costly thread per connection  

Developers see concurrency as a built-in property of Reactor operators rather than manually juggling threads.

### 7.3 Callback Registry, Context Switching, and Why Event Loops Are Fast

Event loops do more than just listen for OS I/O interrupts (epoll on Linux, kqueue on macOS, IOCP on Windows). They also maintain an internal "callback registry." When the OS signals that a socket is ready for reading or writing, the event loop looks up which callback should handle this event. It then rapidly passes control to that callback. Because event loops typically reuse a small number of threads, the overhead of spawning and destroying threads is avoided. This design significantly reduces context switching cost compared to the traditional "one-thread-per-request" model.

Behind the scenes:
• The event loop pulls pending callbacks from an internal queue.  
• Each callback is associated with a reactive operation or chain of operations.  
• The loop executes these callbacks sequentially on the same thread, moving on to new callbacks as soon as one completes or yields.  
• If a blocking operation is needed, Reactor can shift execution to another Scheduler with more suitable threads (e.g., boundedElastic), preventing the event loop from becoming stuck.


### 7.4 Lazy Evaluation and Activation on Subscribe

One core principle of reactive programming is that nothing actually happens until you call
<code>subscribe()</code>. Building a chain of operators (like <code>map</code>, <code>filter</code>, <code>flatMap</code>) does not
execute any I/O or data transformations by itself. Instead, it constructs a pipeline of
“instructions” for how data should be processed once it is requested.

Here is a basic example:

```java
Flux<String> pipeline = Flux.just("dog", "cat", "bird")
    .map(String::toUpperCase)
    .flatMap(name -> doAsyncLookup(name)); 

// At this point, no data is requested and no I/O calls happen.

pipeline.subscribe(
    value -> System.out.println("Got: " + value),
    error -> System.err.println("Error: " + error),
    () -> System.out.println("Completed!")
);
```

Before <code>subscribe()</code> is invoked:
• The <code>Flux</code> is just a blueprint describing how to fetch and transform data.  
• No items are actually fetched.  
• The asynchronous operation in <code>doAsyncLookup</code> is not triggered.

When <code>subscribe()</code> is called:
1. The final <code>Subscriber</code> is attached to the pipeline, establishing a link from downstream to upstream.  
2. The request for data traverses the chain of operators, ultimately reaching the source (<code>Flux.just</code> in this example).  
3. As data flows downstream, each operator (e.g., <code>map</code>, <code>flatMap</code>) applies its logic.  
4. If an operator requires asynchronous I/O, it registers callbacks that pass results back into the pipeline.  
5. Once the final data item is processed and delivered, <code>onComplete</code> is signaled, ending the stream.  

Synchronous operators like <code>map</code> and <code>filter</code> typically run immediately on the calling thread (or on a
thread designated by a <code>Scheduler</code>), applying transformations to each <code>onNext</code> item. Asynchronous operators
like <code>flatMap</code> with an I/O call may break the flow into callbacks handled by an event loop or a separate scheduler
(for example, <code>boundedElastic</code>), but the fundamental principle remains: no operator actually fires until
<code>subscribe()</code> triggers data demand.

---

### 7.5 Shifting Execution to Another Thread: Schedulers

In Project Reactor, a Scheduler controls which thread(s) will run a given piece of reactive logic. By default, publishers run their emissions on the current thread (often the event loop). However, you can use operators like `publishOn` or `subscribeOn` to move downstream or upstream processing to another Scheduler. For example, if a publisher returns in your code and you use `.subscribeOn(Schedulers.boundedElastic())`, future emissions in that chain may run on a different thread from the boundedElastic pool. This approach keeps the main event loop free of long-running tasks.

In short:
- An event loop is fast because it avoids per-request thread creation and only works with callbacks.  
- If complex or blocking tasks are necessary, Reactor’s Schedulers help you shift execution away from the event loop.  
- This model reduces overhead while still providing flexible concurrency when processing I/O events or data transformations.

---

## 8. A High-Level Request Flow in Spring Reactor

1. **Incoming Request**  
   The OS signals an open connection or incoming data on a socket. A Netty event loop picks up the event.  
2. **I/O Parsing**  
   Netty decodes HTTP bytes into an HTTP request object.  
3. **Reactor Pipeline**  
   The request flows into a chain of Reactor operators (e.g., route matching, data parsing).  
4. **Database or ML Inference Calls**  
   Instead of blocking, we return a Publisher that eventually yields results. Meanwhile, the event loop can handle other requests.  
5. **Response Emission**  
   Once the publisher has data, it signals the event loop to write back.  
6. **Completion**  
   The final onComplete signal closes the reactive flow for that request.

All these steps happen on a minimal number of threads, typically fewer than the number of cores.

---

## 9. Under the Hood: OS Notifications

At a deeper level:
• The OS (via epoll on Linux, kqueue on macOS, or IOCP on Windows) informs Netty that a socket is readable/writable.  
• Netty processes these readiness events in an event loop.  
• Reactor’s callbacks handle the logic on top of Netty’s event loop or pass work to extra schedulers as needed.  
• This design avoids idle threads. Instead of blocking, threads only run code when data is present or produce results when requested.

---

## 10. Conclusion and Next Steps

Reactive programming in Java, especially with Project Reactor, rethinks concurrency by removing dedicated blocking threads and embracing asynchronous, non-blocking flows:
• A single event loop can process many can-do tasks concurrently.  
• Light on overhead, straightforward scaling with fewer hardware constraints.  
• Requires you to structure the logic in pipelines of operators rather than sequential blocking code.  

Our next presentation will dive deeper into:
• Best practices for using Reactor effectively  
• Advanced operator patterns and error handling  
• Scheduling strategies and debugging tools  
• Putting it all together with real-world Spring WebFlux examples  

Stay tuned for more detailed explorations of Project Reactor’s internals, best practices, and performance considerations.