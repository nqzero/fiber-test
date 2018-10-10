package com.vlkan.fibertest;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import kilim.Continuation;
import kilim.Scheduler;
import kilim.Task;

public class KilimForkJoin extends Scheduler {
    public static KilimForkJoin sched = new KilimForkJoin();
    ForkJoinPool pool = new ForkJoinPool();

    public void schedule(Task task) {
        ForkJoinPool current = ForkJoinTask.getPool();
        ForkedTask fajita = new ForkedTask(task);
        if (current==pool)
            fajita.fork();
        else
            pool.submit(fajita);
    }
    static final class ForkedTask<V> extends ForkJoinTask<V> {
        Task<V> task;
        public ForkedTask(Task<V> task) { this.task = task; }
        public V getRawResult() { return null; }
        protected void setRawResult(V value) {}
        protected boolean exec() {
            task.run();
            return true;
        }
    }

    public void shutdown() {
        pool.shutdown();
        super.shutdown();
    }
}
