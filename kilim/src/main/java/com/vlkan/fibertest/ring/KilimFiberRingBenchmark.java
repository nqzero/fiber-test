package com.vlkan.fibertest.ring;

import kilim.ForkJoinScheduler;
import kilim.Pausable;
import kilim.PauseReason;
import kilim.Scheduler;
import kilim.Task;
import kilim.tools.Kilim;
import org.openjdk.jmh.annotations.Benchmark;

import static com.vlkan.fibertest.ring.RingBenchmarkConfig.MESSAGE_PASSING_COUNT;
import static com.vlkan.fibertest.ring.RingBenchmarkConfig.THREAD_COUNT;
import static com.vlkan.fibertest.ring.RingBenchmarkConfig.WORKER_COUNT;

/**
 * Ring benchmark using Kilim {@link Task}s with pause-and-resume.
 */
public class KilimFiberRingBenchmark implements RingBenchmark {


    private static final PauseReason PAUSE_REASON = task -> true;

    private static class Worker extends Task<Integer> {

        private final int _id;

        private final int[] sequences;

        private Worker next;

        private int sequence;
        private int setter;

        private Worker(int id, int[] sequences, Scheduler sched) {
            this._id = id;
            this.sequences = sequences;
            setScheduler(sched);
        }

        @Override
        public void execute() throws Pausable {
            while (true) {
                next.setter = setter - 1;
                sequence = setter;
                next.schedule();
                if (sequence <= 0)
                    break;
                Task.pause(PAUSE_REASON);
            }
            sequences[_id] = sequence;
        }
        void schedule() {
            while (! done && ! resume())
                try { nretry++; Thread.sleep(0); } catch (InterruptedException ex) {}
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

        // Initiate the ring.
        Worker firstWorker = workers[0];
        firstWorker.setter = MESSAGE_PASSING_COUNT;
        firstWorker.schedule();

        workers[WORKER_COUNT-1].awaitb();
        for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
            workers[workerIndex].awaitb();
        }

        // Return collected sequences.
        return sequences;

    }
    static Scheduler affine;
    Scheduler sched;
    static int nretry;
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
        int num = 1;
        if (args.length > 0) num = Integer.parseInt(args[0]);
        for (int ii=0; ii < num; ii++) {
            nretry = 0;
            int [] seqs = null;
            if (args.length > 2) seqs = new KilimFiberRingBenchmark().ringBenchmark();
            else                 seqs = new KilimFiberRingBenchmark.Fork().ringBenchmark();
            System.out.format("seq: %5d, retry: %5d\n",seqs[0],nretry);
            if (args.length > 1)
                Thread.sleep(Integer.parseInt(args[1]));
        }
    }

    public static void main(String[] args) throws Exception {
        Kilim.run("com.vlkan.fibertest.ring.KilimFiberRingBenchmark", "kilimEntrace", args);
    }

}
