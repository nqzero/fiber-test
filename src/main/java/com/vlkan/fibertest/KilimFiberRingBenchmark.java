package com.vlkan.fibertest;

import kilim.Pausable;
import kilim.PauseReason;
import kilim.Task;
import kilim.tools.Kilim;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Ring benchmark using Kilim tasks and via pause and resume.
 */
public class KilimFiberRingBenchmark extends AbstractRingBenchmark {
    static PauseReason always = t -> true;

    public static class InternalFiber extends Task<Integer> {
        private final int sid;
        private final int[] sequences;
        private InternalFiber next;
        private int sequence;
        private volatile boolean rung;

        private InternalFiber(int id, int[] sequences) {
            this.sid = id;
            this.sequences = sequences;
            setScheduler(KilimForkJoin.sched);
        }

        @Override
        public void execute() throws Pausable {
            while (true) {
                next.sequence = sequence - 1;
                if (sequence <= 0)
                    rung = true;
                if (! next.rung) next.schedule();
                if (rung)
                    break;
                Task.pause(always);
            }
            sequences[sid] = sequence;
        }
        void schedule() {
            if (! done)
                while (! resume())
                    try { nretry++; Thread.sleep(0); } catch (InterruptedException ex) {}
        }
    }

    static int nretry;
    
    
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
        firstFiber.schedule();

        for (int ii=0; ii < workerCount; ii++)
            fibers[ii].joinb();

        return sequences;

    }

    // allow trampoline detection
    static void dummy() throws Pausable {}    
    
    public static void main(String[] args) {
        if (Kilim.trampoline(true,args)) return;
        int [] seqs = new KilimFiberRingBenchmark().ringBenchmark();
        System.out.format("seq: %5d, retry: %5d\n",seqs[0],nretry);
    }

}
