# OUTLINE

TODO : make outline consistent
TODO : add Backpressure
TODO : Explain simple overview of Project Reactor

Introduction: The Concurrency Challenge
• State the high-level question: How do we handle many concurrent requests efficiently?
Process-Per-Request Model
2.1 What Is a Process?
2.2 Past Approaches (e.g., CGI, Prefork in Apache)
2.3 Where It Breaks Down (Resource overhead, slow context switching, limited scalability)
Enter Threads
3.1 What Is a Thread?
3.2 Thread Resources (stack memory, CPU cycles)
3.3 Thread Limits & Overheads
3.4 Thread-Per-Request Model
– Advantages vs. Past Approaches
– Why It Still Fails Under Heavy Load (context switching, memory usage, thread starvation)
Event Loops
4.1 The Rationale for Event Loops (fewer threads, better I/O concurrency)
4.2 How an Event Loop Works (OS signals, callback registries, single/multi-threaded variants)
4.3 Advantages & Drawbacks (single-threaded bottleneck, concurrency complexities, offloading CPU-intensive tasks)
Spring's Event-Loop Model
5.1 Netty Under the Hood
5.2 How Spring WebFlux Uses Event Loops for I/O
5.3 Request Flow in a Reactive Spring Application (brief high-level walkthrough)
Enter Reactive Programming
6.1 The Reactive Manifesto & Its Principles
– Responsive, Resilient, Elastic, Message-Driven
6.2 Reactive Streams Specification (Publisher, Subscriber, Subscription, Processor)
6.3 Project Reactor Overview
– Key Operators (map, flatMap, filter)
– Lazy evaluation (activation on subscribe)
Advantages & Disadvantages of Project Reactor
7.1 Benefits (optimal resource usage, concurrency, fewer idle threads)
7.2 Trade-Offs (steep learning curve, debugging complexity, mindset shift)
Example Code
• Show a minimal Reactor flow (Flux / Mono usage)
• Demonstrate lazy execution until subscribe()
Summary & Next Steps
• Summarize the evolution from processes to threads to event loops
• Emphasize how Reactor builds on the event-loop model
• Preview deeper dives: advanced operators, debugging, best practices

# 1. Introduction: The Concurrency Challenge


---

This training addresses the overarching challenge: How do we handle many concurrent requests efficiently without exhausting system resources?

# 2. Process-Per-Request Model

## 2.1 What Is a Process?
A process is an instance of a running program. It has its own dedicated memory space (including heap and stack), file descriptors, and OS-level resources. Early solutions spawned a process for each request, as shown below.

---

## 2.2 Past Approaches: CGI & Prefork (Historical Context)

In older systems (e.g., early Unix servers):
- **CGI scripts**: [A child process handled dynamic content for each request](#CGI).  
- **Prefork**: [A pool of processes was ready to take connections (Apache HTTPD)](#Prefork).

Processes are expensive in memory usage and [context switching](#context-switching-overhead), eventually hitting scaling limits.

---

## 2.3 Where It Breaks Down
• High resource overhead (each process has its own memory space).  
• [Context switching](#context-switching-overhead) among processes is costly.  
• Frequent forking or process creation depletes resources.

---

# 3. Enter Threads

## 3.1 What Is a Thread?
A thread is a subdivision of a process, sharing the same memory but maintaining its own stack and register states. Threads are lighter than processes and pave the way for concurrency with less overhead than process-per-request.

## 3.2 Thread Resources, Limits, & Overheads
- **Stack Memory**: Each thread has a stack (e.g., 256KB – 1MB in many JVMs).  
- **CPU**: Time spent creating/destroying threads, plus [context switching](#context-switching-overhead).  
- **Limits**: OS constraints and practical overhead keep thread counts from growing indefinitely.


## 3.3 Thread-Per-Request Model
Many traditional Java servers mapped each request to its own thread. Under moderate load, this was workable. But at scale:
• Huge memory consumption (lots of thread stacks).  
• High [context-switch](#context-switching-overhead) overhead.  
• Possible thread starvation.

---

# 4. Event Loops
Event loops address these problems by using a small pool of threads to handle many concurrent I/O operations. Instead of blocking a separate thread per request, an event loop runs callbacks triggered by OS signals (e.g., data ready to read).

## 4.1 Rationale for Event Loops
• Fewer threads handle many connections via asynchronous I/O.  
• Lower memory usage (few thread stacks).  
• Reduced context switching vs. thread-per-request.

## 4.2 How an Event Loop Works
- Waits for OS signals (epoll, kqueue, IOCP).  
- Picks a callback from a queue when I/O is ready.  
- Executes callbacks sequentially (single-thread model) or across a small pool.  
- Non-blocking tasks keep loops free for more I/O.

[More Details](#more-about-event-loops)

## 4.3 Advantages & Drawbacks
**Advantages**: Minimal threads, high I/O throughput, simpler concurrency (less locking).  
**Drawbacks**: CPU-bound tasks must not block the loop; more complex code design (callbacks).

---

# 5. Spring's Event-Loop Model

## 5.1 Netty Under the Hood
Spring WebFlux relies on Netty for its networking. Netty uses multiple event loop groups to handle inbound/outbound I/O across available CPU cores.

## 5.2 How Spring WebFlux Uses Event Loops
- On each I/O event, the Netty loop triggers Reactor operators or passes control to a Scheduler if blocking tasks are offloaded.

## 5.3 Request Flow in a Reactive Spring Application
1. OS signals new request.  
2. Netty decodes the request.  
3. Reactor pipeline processes data asynchronously.  
4. If I/O (DB calls, ML inference) is needed, it returns immediately, letting the loop serve other requests.  
5. Once data is ready, the loop completes the response.

---

# 6. Enter Reactive Programming

## 6.1 The Reactive Manifesto & Its Principles
- Responsive, Resilient, Elastic, Message-Driven.  
- Sets guidelines on how modern systems should behave under load and failure.

## 6.2 Reactive Streams Specification
- Defines Publisher, Subscriber, Subscription, Processor.  
- Ensures [backpressure](#Backpressure) and async I/O across different libraries.

Two popular Java libraries are:
• **[Project Reactor](https://projectreactor.io/docs/core/3.3.22.RELEASE/reference/index.html#about-doc)** (part of the Spring ecosystem)  
• **RxJava** (inspired by Reactive Extensions from Microsoft)


## 6.3 Project Reactor Overview
- Part of Spring ecosystem, tightly integrated with WebFlux.  
- Uses non-blocking operators (map, flatMap) to transform data.  
- Lazy evaluation: Nothing runs until `subscribe()`.
- Developers see concurrency as a built-in property of Reactor operators rather than manually 
juggling threads.

---

# 7. Advantages & Disadvantages of Project Reactor
- **Advantages**: Fewer idle threads, high concurrency, scales easily with I/O.  
- **Drawbacks**: Debugging async flows can be harder; requires a paradigm shift.

---

# 8. Summary & Next Steps
- Summarize: Evolved from process-per-request → thread-per-request → event loops + reactive.  
- Reactor harnesses event loops for non-blocking concurrency.  
- Next steps: deeper exploration of advanced operators, debugging, scheduling strategies.

---

# APPENDIX

### CGI
- **Early CGI scripts** (Common Gateway Interface). A CGI script is an external program run by 
the web server to generate dynamic responses. When a request arrived that needed dynamic content, 
the server would "fork" a child process and execute the CGI program. Once the program finished 
generating output (HTML, for instance), the process would terminate. This approach was simpler to 
implement in early web servers but created significant overhead due to frequent process creation 
and teardown.

### Prefork
- **Prefork models** in some HTTP servers (e.g., older versions of Apache HTTPD). Under this 
model, the server would start ("prefork") a pool of child processes on startup. Each child 
process waited for incoming connections. When a request came in, the server assigned it to one of 
these existing processes, avoiding the overhead of creating a fresh process for every request. 
Even though this was more efficient than forking a new process on every request, each child still 
consumed more resources than a thread. Handling hundreds or thousands of connections could lead 
to huge memory usage because every process had its own memory space, file descriptors, etc.

### Context Switching Overhead

• **Definition**: Context switching is when the CPU switches from executing one thread to 
another.  
• **Overhead**: Every switch involves saving and loading CPU registers, updating memory maps, and 
cache invalidation. Because each thread can have its own state (registers, stack pointer, program 
counter, etc.), the operating system must temporarily store these details and load the next 
thread's equivalent details. Additionally, some or all of the CPU's internal caches (such as data 
caches and translation lookaside buffers, or TLBs) may be flushed or become outdated. A context 
switch can also disrupt the CPU's instruction pipeline, forcing it to refill from scratch. All 
these factors contribute to time spent not doing useful work.  
  
Frequent context switches are particularly detrimental to performance because:
1. **CPU Time Spent in Scheduling**: The OS scheduler must decide which thread to run next, 
adding overhead for scheduling algorithms.  
2. **Cache Miss Penalties**: Once the switch occurs, the new thread's memory references may not 
be in the cache anymore, leading to additional stalls.  
3. **Pipeline Flushes**: Modern CPUs rely on deep pipelines and speculative execution. Switching 
threads can hamper these optimizations, forcing them to restart.  
4. **RAM and TLB Overheads**: Accessing memory pages for the newly scheduled thread can force TLB 
reloads, further slowing execution.  
  
When context switching happens rarely (for example, if long-running tasks occupy a thread for a 
while), the overhead is less noticeable. However, under highly concurrent loads with hundreds or 
thousands of threads constantly preempting each other, context switching can snowball into a 
major performance bottleneck.

### More About Event Loops

Event loops do more than just listen for OS I/O interrupts (epoll on Linux, kqueue on macOS, IOCP 
on Windows). They also maintain an internal "callback registry." When the OS signals that a 
socket is ready for reading or writing, the event loop looks up which callback should handle this 
event. It then rapidly passes control to that callback. Because event loops typically reuse a 
small number of threads, the overhead of spawning and destroying threads is avoided. This design 
significantly reduces context switching cost compared to the traditional "one-thread-per-request" 
model.

Behind the scenes:
• The event loop pulls pending callbacks from an internal queue.  
• Each callback is associated with a reactive operation or chain of operations.  
• The loop executes these callbacks sequentially on the same thread, moving on to new callbacks 
as soon as one completes or yields.  
• If a blocking operation is needed, Reactor can shift execution to another Scheduler with more 
suitable threads (e.g., boundedElastic), preventing the event loop from becoming stuck.

### Backpressure

Backpressure is the mechanism by which a Subscriber can signal to a Publisher that it can only handle
a certain number of items at a time, preventing the Publisher from overwhelming the consumer with
excess data. This concept comes from the [Reactive Streams specification](https://github.com/reactive-streams/reactive-streams-jvm/blob/v1.0.4/README.md#specification) and is central to ensuring
that asynchronous, possibly unbounded data flows remain controllable and efficient.

#### Motivation
Often, the producer (Publisher) can generate data faster than the consumer (Subscriber) can process it.
Without backpressure, the consumer might run out of memory or otherwise become unresponsive, because
the producer keeps sending data it cannot handle in time. Backpressure lets the consumer dictate
the pace, avoiding resource exhaustion.

#### Reactive Streams Approach
The Reactive Streams specification formalizes backpressure using a dedicated interface called
Subscription. Once a Subscriber subscribes to a Publisher, it receives a Subscription, which it can
use to request a certain number of items via the <code>request(long n)</code> method:

1. **Publisher**: Responsible for producing items.  
2. **Subscriber**: Consumes items. It can ask for more items incrementally to avoid being flooded.  
3. **Subscription**: Maintains the relationship between publisher and subscriber; the subscriber
   calls <code>request(n)</code> to demand that many more items, and the publisher calls
   <code>onNext</code> up to <code>n</code> times (unless the sequence completes faster).

#### How It Works in Project Reactor
Project Reactor fully implements this backpressure model in the chain of operators that transform
data between the original source Publisher and the final Subscriber. When you compose operators
(e.g., <code>map</code>, <code>filter</code>, <code>flatMap</code>), each operator itself can act as both a Subscriber to
the preceding stage and a Publisher for the next stage. Each operator:
• Responds to <code>request(n)</code> calls from its downstream consumer.  
• Propagates these requests upstream so that only <code>n</code> items are demanded at each step in the pipeline.  
• Optionally implements strategies like buffering, dropping, or erroring out if data arrives
  faster than it can handle.

#### Example Chain of Calls
1. The final Subscriber calls <code>request(n)</code> on its Subscription.  
2. That Subscription belongs to an operator (e.g., <code>filter</code>), which forwards <code>request(n)</code> to
   its upstream Subscription.  
3. This propagates up the chain until it reaches the original Publisher.  
4. The Publisher emits up to <code>n</code> items downstream.  
5. Each operator processes these items, possibly buffering or transforming them, and passes them along
   via <code>onNext</code> calls eventually reaching the final Subscriber.

#### Backpressure Strategies
When the consumer can't keep up, different operators or Publishers may adopt various behaviors:
• **Buffer** extra data until the consumer calls <code>request</code> again.  
• **Drop** new data (e.g., <code>onBackpressureDrop</code>) to avoid using too much memory.  
• **Latest** keep only the most recent item and drop older items (e.g., <code>onBackpressureLatest</code>).  
• **Error** fail the stream if it can't process items quickly enough.

#### Why It Matters
Backpressure ensures robust resource usage in asynchronous systems. In Project Reactor, this
fine-grained flow control is automatic if you follow the Reactive Streams contract. By respecting
the <code>request</code> mechanism, your application remains stable and predictable under varying
load conditions. It aligns well with the "Responsive" and "Resilient" tenets of the Reactive
Manifesto, ensuring that a slow or overloaded subscriber does not bring down the entire system.

### Schedulers For Shifting Execution to Another Thread

In Project Reactor, a Scheduler controls which thread(s) will run a given piece of reactive 
logic. By default, publishers run their emissions on the current thread (often the event loop). 
However, you can use operators like `publishOn` or `subscribeOn` to move downstream or upstream 
processing to another Scheduler. For example, if a publisher returns in your code and you use `.
subscribeOn(Schedulers.boundedElastic())`, future emissions in that chain may run on a different 
thread from the boundedElastic pool. This approach keeps the main event loop free of long-running 
tasks.

In short:
- An event loop is fast because it avoids per-request thread creation and only works with 
callbacks.  
- If complex or blocking tasks are necessary, Reactor's Schedulers help you shift execution away 
from the event loop.  
- This model reduces overhead while still providing flexible concurrency when processing I/O 
events or data transformations.


### Lazy Evaluation and Activation on Subscribe

One core principle of reactive programming is that nothing actually happens until you call
<code>subscribe()</code>. Building a chain of operators (like <code>map</code>, <code>filter</code>, <code>flatMap</code>) does not
execute any I/O or data transformations by itself. Instead, it constructs a pipeline of
"instructions" for how data should be processed once it is requested.

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