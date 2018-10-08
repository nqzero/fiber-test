package com.vlkan.fibertest;

import kilim.Cell;
import kilim.Pausable;
import kilim.Scheduler;
import kilim.Task;
import kilim.tools.Kilim;
import org.openjdk.jmh.annotations.Benchmark;

/**
 * Ring benchmark using Kilim tasks and message passing, ie the Kilim metaphor for an actor.
 */
public class KilimActorRingBenchmark extends AbstractRingBenchmark {
    static { Scheduler.defaultNumberThreads = 1; }

    public static class InternalFiber extends Task<Integer> {
        private final int sid;
        private final int[] sequences;
        private InternalFiber next;
        private int sequence;
        private Cell<Integer> box = new Cell();
        

        private InternalFiber(int id, int[] sequences) {
            this.sid = id;
            this.sequences = sequences;
        }

        @Override
        public void execute() throws Pausable {
            do {
                sequence = box.get();
                next.box.putnb(sequence - 1);
            } while (sequence > 0);
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

        // Start fibers.
        for (InternalFiber fiber : fibers) {
            fiber.start();
        }

        // Initiate the ring.
        InternalFiber firstFiber = fibers[0];
        firstFiber.box.putnb(ringSize);

        Task.idledown();
        return sequences;

    }
    
    public static void main(String[] args) {
        if (Kilim.trampoline(true,args)) return;
        new KilimActorRingBenchmark().ringBenchmark();
    }


}
