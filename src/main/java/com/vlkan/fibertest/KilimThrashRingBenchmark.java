package com.vlkan.fibertest;

import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import kilim.Continuation;
import kilim.Fiber;
import kilim.Pausable;
import kilim.Scheduler;
import kilim.tools.Kilim;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Ring benchmark using Kilim continuations.
 */
public class KilimThrashRingBenchmark extends AbstractRingBenchmark {

    static int num = Scheduler.defaultNumberThreads;
    static LinkedBlockingQueue<InternalFiber> cooked = new LinkedBlockingQueue();

    static class Runner extends Thread {
        Queue<InternalFiber> queue = num==1 ? new LinkedList() : new ConcurrentLinkedQueue();

        @Override
        public void run() {
            while (true) {
                InternalFiber cc = queue.poll();
                if (cc != null)
                    if (cc.run())
                        cooked.add(cc);
            }
        }
    }

    static int index = -1;
    static Runner[] runners;
    static void resume(InternalFiber task) {
        if (task.done) return;
        if (++index==num) index = 0;
        runners[index].queue.add(task);
    }
    static boolean prep() {
        if (runners != null)
            return false;
        runners = new Runner[num];
        for (int ii=0; ii < num; ii++)
            runners[ii] = new Runner();
        for (Runner runner : runners)
            runner.setDaemon(true);
        return true;
    }
    static void start() {
        for (Runner runner : runners)
            runner.start();
    }
    
    
    public static class InternalFiber extends Continuation {
        private final int sid;
        private final int[] sequences;
        private InternalFiber next;
        private int sequence;
        private boolean done;
        

        private InternalFiber(int id, int[] sequences) {
            this.sid = id;
            this.sequences = sequences;
        }

        @Override
        public void execute() throws Pausable {
            while (true) {
                next.sequence = sequence - 1;
                resume(next);
                if (sequence <= 0)
                    break;
                Fiber.yield();
            }
            sequences[sid] = sequence;
            done = true;
        }
    }

    @Override
    @Benchmark
    public int[] ringBenchmark() {
        boolean start = prep();
        
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
        
        if (start) start();

        try {
            for (int ii=0; ii < workerCount; ii++)
                cooked.take();
        }
        catch (InterruptedException ex) {}
        return sequences;
    }
    
    // allow trampoline detection
    static void dummy() throws Pausable {}    
    
    public static void main(String[] args) {
        if (Kilim.trampoline(true,args)) return;
        int [] seqs = new KilimThrashRingBenchmark().ringBenchmark();
        System.out.println("seq: " + seqs[0]);
    }


}
