package com.vlkan.fibertest;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import kilim.Continuation;
import kilim.Fiber;
import kilim.NotPausable;
import kilim.Pausable;
import kilim.tools.Kilim;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Ring benchmark using Kilim continuations.
 */
public class KilimContinuationRingBenchmark extends AbstractRingBenchmark {

    static BlockingQueue<InternalFiber> done = new LinkedBlockingQueue();
    
    static int nretry;
    static void resume(InternalFiber task) {
        while (! task.ready)
            try { nretry++; Thread.sleep(0); } catch (InterruptedException ex) {}
        if (! task.done)
            KilimForkJoin.sched.schedule(task,done::add);
    }
    
    public static class InternalFiber extends Continuation {
        private final int sid;
        private final int[] sequences;
        private InternalFiber next;
        private int sequence;
        private boolean done;
        private volatile boolean ready = true;
        

        private InternalFiber(int id, int[] sequences) {
            this.sid = id;
            this.sequences = sequences;
        }

        @Override
        public void execute() throws Pausable {
            while (true) {
                next.sequence = sequence - 1;
                if (sequence <= 0)
                    done = true;
                resume(next);
                if (done)
                    break;
                Fiber.yield();
            }
            sequences[sid] = sequence;
        }

        public boolean run() throws NotPausable {
            ready = false;
            boolean ret = super.run();
            ready = true;
            return ret;
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
        resume(firstFiber);
        try {
            for (int ii=0; ii < workerCount; ii++)
                done.take();
        }
        catch (InterruptedException ex) {}

        return sequences;

    }
    
    public static void main(String[] args) {
        if (Kilim.trampoline(true,args)) return;
        int [] seqs = new KilimContinuationRingBenchmark().ringBenchmark();
        System.out.format("seq: %5d, retry: %5d\n",seqs[0],nretry);
    }


}
