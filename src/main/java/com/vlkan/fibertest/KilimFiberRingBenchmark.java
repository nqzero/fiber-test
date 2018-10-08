package com.vlkan.fibertest;

import kilim.Pausable;
import kilim.PauseReason;
import kilim.Scheduler;
import kilim.Task;
import kilim.tools.Kilim;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Ring benchmark using Kilim tasks and via pause and resume.
 */
public class KilimFiberRingBenchmark extends AbstractRingBenchmark {
    static { Scheduler.defaultNumberThreads = 1; }
    static PauseReason always = t -> true;

    public static class InternalFiber extends Task<Integer> {
        private final int sid;
        private final int[] sequences;
        private InternalFiber next;
        private int sequence;

        private InternalFiber(int id, int[] sequences) {
            this.sid = id;
            this.sequences = sequences;
            setScheduler(Scheduler.getDefaultScheduler());
        }

        @Override
        public void execute() throws Pausable {
            while (true) {
                next.sequence = sequence - 1;
                next.resume();
                if (sequence <= 0)
                    break;
                Task.pause(always);
            }
            sequences[sid] = sequence;
        }
    }

    @Override
    @Benchmark
    public int[] ringBenchmark() {

        // Create fibers.
        int[] sequences = new int[workerCount];
        InternalFiber[] fibers = new InternalFiber[workerCount];
        for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
            fibers[workerIndex] = new InternalFiber(workerIndex, sequences);
        }

        // Set next fiber pointers.
        for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
            fibers[workerIndex].next = fibers[(workerIndex + 1) % workerCount];
        }

        // Initiate the ring.
        InternalFiber firstFiber = fibers[0];
        firstFiber.sequence = ringSize;
        firstFiber.resume();

        for (int ii=0; ii < workerCount; ii++)
            fibers[ii].joinb();

        Scheduler.getDefaultScheduler().shutdown();
        return sequences;

    }

    @SuppressWarnings("unused")     // entrance for Kilim.run()
    public static void kilimEntrace(String[] ignored) {
        new KilimFiberRingBenchmark().ringBenchmark();
    }

    public static void main(String[] args) throws Exception {
        Kilim.run("com.vlkan.fibertest.KilimFiberRingBenchmark", "kilimEntrace", args);
    }

}
