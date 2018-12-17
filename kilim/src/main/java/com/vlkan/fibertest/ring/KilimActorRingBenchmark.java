package com.vlkan.fibertest.ring;

import kilim.Cell;
import kilim.ForkJoinScheduler;
import kilim.Pausable;
import kilim.Scheduler;
import kilim.Task;
import kilim.tools.Kilim;
import org.openjdk.jmh.annotations.Benchmark;

import static com.vlkan.fibertest.ring.RingBenchmarkConfig.MESSAGE_PASSING_COUNT;
import static com.vlkan.fibertest.ring.RingBenchmarkConfig.THREAD_COUNT;
import static com.vlkan.fibertest.ring.RingBenchmarkConfig.WORKER_COUNT;

/**
 * Ring benchmark using Kilim {@link Task}s with message passing.
 */
public class KilimActorRingBenchmark implements RingBenchmark {


    private static class Worker extends Task<Integer> {

        private final int _id;

        private final int[] sequences;

        private Worker next;

        private int sequence;

        private Cell<Integer> box = new Cell();

        private Worker(int id, int[] sequences, Scheduler sched) {
            this._id = id;
            this.sequences = sequences;
            setScheduler(sched);
        }

        @Override
        public void execute() throws Pausable {
            do {
                sequence = box.get();
                next.box.putnb(sequence - 1);
            } while (sequence > 0);
            sequences[_id] = sequence;
        }
        void awaitb() {
            if (! done) joinb();
        }
    }

    @Override
    @Benchmark
    public int[] ringBenchmark() {
        setup();

        // Create workers.
        int[] sequences = new int[WORKER_COUNT];
        Worker[] workers = new Worker[WORKER_COUNT];
        for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
            workers[workerIndex] = new Worker(workerIndex, sequences, sched);
        }

        // Set next worker pointers.
        for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
            workers[workerIndex].next = workers[(workerIndex + 1) % WORKER_COUNT];
        }

        // Start workers.
        for (Worker worker : workers) {
            worker.start();
        }

        // Initiate the ring.
        Worker firstWorker = workers[0];
        firstWorker.box.putnb(MESSAGE_PASSING_COUNT);

        // Wait for scheduler to finish and shut it down.
        workers[WORKER_COUNT-1].awaitb();
        for (int ii=0; ii < WORKER_COUNT; ii++)
            workers[ii].awaitb();

        // Return populated sequences.
        return sequences;

    }

    static Scheduler affine;
    Scheduler sched;
    void setup() {
        if (sched==null) {
            if (affine==null)
                affine = Scheduler.make(THREAD_COUNT);
            sched = affine;
        }
    }
    
    public static class Fork extends KilimFiberRingBenchmark {
        static Scheduler fork = new ForkJoinScheduler(THREAD_COUNT);
        { sched = fork; }
    }
    

    @SuppressWarnings("unused")     // entrance for Kilim.run()
    public static void kilimEntrace(String[] args) throws Exception {
        if (Kilim.trampoline(true,args)) return;
        int num = 1;
        if (args.length > 0) num = Integer.parseInt(args[0]);
        for (int ii=0; ii < num; ii++) {
            int [] seqs = null;
            if (args.length > 2) seqs = new KilimActorRingBenchmark().ringBenchmark();
            else                 seqs = new KilimActorRingBenchmark.Fork().ringBenchmark();
            System.out.format("seq: %5d\n",seqs[0]);
            if (args.length > 1)
                Thread.sleep(Integer.parseInt(args[1]));
        }
    }

    public static void main(String[] args) throws Exception {
        Kilim.run("com.vlkan.fibertest.ring.KilimActorRingBenchmark", "kilimEntrace", args);
    }

}
