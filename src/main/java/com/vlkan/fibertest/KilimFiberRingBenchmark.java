package com.vlkan.fibertest;

import kilim.ForkJoinScheduler;
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
    static PauseReason always = t -> true;
    static Scheduler affine;
    Scheduler sched;

    void setup() {
        if (sched==null) {
            if (affine==null)
                affine = Scheduler.make(1);
            sched = affine;
        }
    }
    
    public static class Fork extends KilimFiberRingBenchmark {
        static Scheduler fork = new ForkJoinScheduler(-1);
        { sched = fork; }
    }
    
    public static class InternalFiber extends Task<Integer> {
        private final int sid;
        private final int[] sequences;
        private InternalFiber next;
        private int sequence;
        private int setter;

        private InternalFiber(int id, int[] sequences, Scheduler sched) {
            this.sid = id;
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
                Task.pause(always);
            }
            sequences[sid] = sequence;
        }
        void schedule() {
            while (! done && ! resume())
                try { nretry++; Thread.sleep(0); } catch (InterruptedException ex) {}
        }
        void awaitb() {
            if (! done) joinb();
        }
    }

    static int nretry;
    
    
    @Override
    @Benchmark
    public int[] ringBenchmark() {
        setup();

        // Create fibers.
        int[] sequences = new int[workerCount];
        InternalFiber[] fibers = new InternalFiber[workerCount];
        for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
            fibers[workerIndex] = new InternalFiber(workerIndex, sequences, sched);
        }

        // Set next fiber pointers.
        for (int workerIndex = 0; workerIndex < workerCount; workerIndex++) {
            fibers[workerIndex].next = fibers[(workerIndex + 1) % workerCount];
        }

        // Initiate the ring.
        InternalFiber firstFiber = fibers[0];
        firstFiber.setter = ringSize;
        firstFiber.schedule();

        fibers[workerCount-1].awaitb();
        for (int ii=0; ii < workerCount; ii++)
            fibers[ii].awaitb();

        return sequences;

    }

    // allow trampoline detection
    static void dummy() throws Pausable {}    
    
    public static void main(String[] args) throws Exception {
        if (Kilim.trampoline(true,args)) return;
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

}
