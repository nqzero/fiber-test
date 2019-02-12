package com.vlkan.fibertest.ring;

import static com.vlkan.fibertest.StdoutLogger.log;
import kilim.ForkJoinScheduler;
import kilim.Pausable;
import kilim.PauseReason;
import kilim.Scheduler;
import kilim.Task;
import kilim.tools.Kilim;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import static com.vlkan.fibertest.ring.RingBenchmarkConfig.MESSAGE_PASSING_COUNT;
import static com.vlkan.fibertest.ring.RingBenchmarkConfig.THREAD_COUNT;
import static com.vlkan.fibertest.ring.RingBenchmarkConfig.WORKER_COUNT;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import kilim.Cell;

/**
 * Ring benchmark using Kilim {@link Task}s with pause-and-resume.
 */
@State(Scope.Benchmark)
public class KilimFiberRingBenchmark implements RingBenchmark {
    private static final PauseReason PAUSE_REASON = task -> true;
    private enum Completed { INSTANCE }

    static AtomicInteger errorCount = new AtomicInteger();
    
    private static class Worker extends Task<Integer> {
        private final int _id;
        private final Cell<Completed> completedCell;
        private Worker next;
        private int sequence;
        private int setter;
        AtomicInteger lock = new AtomicInteger();

        private Worker(int id, Cell<Completed> completedCell, Scheduler scheduler) {
            this._id = id;
            this.completedCell = completedCell;
            setScheduler(scheduler);
        }

        // for locking vs atomic tracking vs vanilla
        //     ST case, 29s vs 13s vs 11s, 1/50
        //     MT case, 108s vs 26s vs 21s, 3/71
        void lock() {
            if (! lock.compareAndSet(0,1))
                errorCount.incrementAndGet();
        }
        void unlock() {
            if (! lock.compareAndSet(1,0))
                errorCount.incrementAndGet();
        }
        
        @Override
        public void execute() throws Pausable {
            while (true) {
                Task.pause(PAUSE_REASON);
                this.lock();
                next.lock();
                next.setter = setter - 1;
                next.unlock();
                sequence = setter;
                this.unlock();
                if (sequence <= 0) {
                    log("[%2d] completed (sequence=%d)", () -> new Object[] { _id, sequence });
                    completedCell.put(Completed.INSTANCE);
                } else {
                    log("[%2d] signaling next (sequence=%d)", () -> new Object[]{_id, sequence});
                    next.schedule();
                }
            }
        }
        
        int get() {
            lock();
            int seq = sequence;
            unlock();
            return seq;
        }
        void schedule() {
            while (! resume())
                try { Thread.sleep(0); } catch (InterruptedException ex) {}
        }
        @Override
        public String toString() {
            return "Worker-" + _id;
        }
    }

    private static final class Context implements AutoCloseable, Callable<int[]> {
        private final Scheduler scheduler;
        private final int[] sequences = new int[WORKER_COUNT];
        private final Worker[] workers;
        private final Cell<Completed> completedCell = new Cell<>();

        private Context() {
            scheduler =  THREAD_COUNT==0
                    ? Scheduler.make(1)
                    : new ForkJoinScheduler(THREAD_COUNT);

            log("creating workers (WORKER_COUNT=%d)", WORKER_COUNT);
            this.workers = new Worker[WORKER_COUNT];
            for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
                workers[workerIndex] = new Worker(workerIndex, completedCell, scheduler);
            }

            log("setting next worker pointers");
            for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
                workers[workerIndex].next = workers[(workerIndex + 1) % WORKER_COUNT];
            }

            log("starting workers");
            for (Worker worker : workers) {
                worker.schedule();
            }
        }

        @Override
        public void close() {
            log("shutting down scheduler");
            scheduler.shutdown();
        }

        @Override
        public int[] call() {
            log("initiating ring (MESSAGE_PASSING_COUNT=%d)", MESSAGE_PASSING_COUNT);
            Worker firstWorker = workers[0];
            firstWorker.lock();
            firstWorker.setter = MESSAGE_PASSING_COUNT;
            firstWorker.unlock();
            firstWorker.schedule();

            log("waiting for completion");
            completedCell.getb();

            log("collecting sequences");
            for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
                sequences[workerIndex] = workers[workerIndex].get();
            }

            log("returning collected sequences (sequences=%s)", () -> new Object[] { Arrays.toString(sequences) });
            return sequences;

        }

    }

    private final Context context = new Context();

    @Override
    @TearDown
    public void close() {
        context.close();
    }

    @Override
    @Benchmark
    public int[] ringBenchmark() {
        int [] seqs = context.call();
        if (errorCount.get() > 0)
            throw new RuntimeException("Ring Lock Error: " + errorCount.get());
        return seqs;
    }
    private static void verifyResult(int[] sequences) {
        int completedWorkerIndex = MESSAGE_PASSING_COUNT % WORKER_COUNT;
        int workerIndex = completedWorkerIndex;
        int expectedSequence = 0;
        do {
            int actualSequence = sequences[workerIndex];
            if (expectedSequence != actualSequence)
                throw new RuntimeException("Ring Mismatch Exception: " + workerIndex);
            expectedSequence++;
            workerIndex = (workerIndex - 1 + WORKER_COUNT) % WORKER_COUNT;
        } while (workerIndex != completedWorkerIndex && expectedSequence <= MESSAGE_PASSING_COUNT);
    }

    @SuppressWarnings("unused")     // entrance for Kilim.run()
    public static void kilimEntrace(String[] ignored) {
        try (KilimFiberRingBenchmark benchmark = new KilimFiberRingBenchmark()) {
            for (int ii=0; ii <= 50; ii++) {
                int [] seqs = benchmark.ringBenchmark();
                if (ii%10==0)
                    verifyResult(seqs);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Kilim.run("com.vlkan.fibertest.ring.KilimFiberRingBenchmark", "kilimEntrace", args);
    }

}
