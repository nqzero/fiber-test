package com.vlkan.fibertest.ring;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.strands.dataflow.Var;
import org.openjdk.jmh.annotations.Benchmark;

import static com.vlkan.fibertest.ring.RingBenchmarkConfig.MESSAGE_PASSING_COUNT;
import static com.vlkan.fibertest.ring.RingBenchmarkConfig.WORKER_COUNT;

/**
 * Ring benchmark using Quasar {@link Fiber}s with {@link Var}s.
 */
public class QuasarDataflowRingBenchmark implements RingBenchmark {

    private static class InternalFiber extends Fiber<Integer> {

        private Var<Integer> current = new Var<>();

        private Var<Integer> next;

        private InternalFiber(int id) {
            super(String.format("%s-%s-%d",
                    QuasarChannelRingBenchmark.class.getSimpleName(),
                    InternalFiber.class.getSimpleName(), id));
        }

        @Override
        protected Integer run() throws SuspendExecution, InterruptedException {
            Integer sequence;
            do {
                sequence = current.getNext();
                next.set(sequence - 1);
            } while (sequence > 0);
            return sequence;
        }

    }

    @Override
    @Benchmark
    public int[] ringBenchmark() throws Exception {

        // Create fibers.
        InternalFiber[] fibers = new InternalFiber[WORKER_COUNT];
        for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
            fibers[workerIndex] = new InternalFiber(workerIndex);
        }

        // Set next fiber pointers.
        for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
            fibers[workerIndex].next = fibers[(workerIndex + 1) % WORKER_COUNT].current;
        }

        // Start fibers.
        for (InternalFiber fiber : fibers) {
            fiber.start();
        }

        // Initiate the ring.
        InternalFiber firstFiber = fibers[0];
        firstFiber.current.set(MESSAGE_PASSING_COUNT);

        // Wait for fibers to complete.
        int[] sequences = new int[WORKER_COUNT];
        for (int workerIndex = 0; workerIndex < WORKER_COUNT; workerIndex++) {
            sequences[workerIndex] = fibers[workerIndex].get();
        }

        // Return populated sequences.
        return sequences;

    }

    public static void main(String[] args) throws Exception {
        new QuasarDataflowRingBenchmark().ringBenchmark();
    }

}
