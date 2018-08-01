package info.jerrinot.loom;

import org.junit.Test;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class FiberTest {
    @Test
    public void testParking() throws Exception {
        int fiberCount = 128;

        Executor scheduler = Executors.newFixedThreadPool(4);
        BlockingQueue<Fiber> pausedFiberQueue = new LinkedBlockingDeque<>();
        for (int i = 0; i < fiberCount; i++) {
            Fiber.execute(scheduler, new PoliteRunnable(pausedFiberQueue));
        }

        for (; ; ) {
            Fiber fiber = pausedFiberQueue.take();
            fiber.unpark();
        }
    }

    @Test
    public void testBlockingQueue() throws Exception {
        int queueCount = 1024;
        int threadCount = 4;

        Executor scheduler = Executors.newFixedThreadPool(threadCount);
        BlockingQueue<Object>[] queues = new BlockingQueue[queueCount];
        for (int i = 0; i < queueCount; i++) {
            BlockingQueue<Object> queue = new LinkedBlockingDeque<>();
            Fiber.execute(scheduler, new ConsumingRunnable(queue));
            queues[i] = queue;
        }

        for (;;) {
            BlockingQueue<Object> randomQueue = queues[ThreadLocalRandom.current().nextInt(queueCount)];
            randomQueue.offer(ThreadLocalRandom.current().nextInt());
            Thread.sleep(1000);
        }
    }

    @Test
    public void testAffinity() throws Exception {
        int queueCount = 1024;
        int threadCount = 2;

        Executor scheduler = Executors.newFixedThreadPool(threadCount);
        Fiber f1 = Fiber.execute(scheduler, new PrintAndYield());

        //sleep to give time f1 to yield
        Thread.sleep(1000);

        //at this point f1 is parked. let's start another thread.
        Fiber f2 = Fiber.execute(scheduler, new PrintAndYield());


        Fiber f3 = Fiber.execute(scheduler, new MonopolizeThread());
        Thread.sleep(1000);

        f1.unpark();
        f2.unpark();


        Thread.sleep(100000);
    }

    public static class MonopolizeThread implements Runnable {
        @Override
        public void run() {
            System.out.println("Monopolozing thread " + Fiber.currentStrand());
            for (;;) {

            }
        }
    }

    public static class PrintAndYield implements Runnable {
        private static final AtomicInteger ID_GENERATOR = new AtomicInteger();
        private long id = ID_GENERATOR.getAndIncrement();

        @Override
        public void run() {
            synchronized ("uberlock") {
                System.out.println("Fiber " + id + " just started on a thread " + Fiber.currentStrand());
                Fiber.park();
                System.out.println("Fiber " + id + " was just woke-up on a thread" + Fiber.currentStrand());
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    public static class ConsumingRunnable implements Runnable {
        private static final AtomicInteger ID_GENERATOR = new AtomicInteger();

        private final BlockingQueue<Object> queue;
        private long id = ID_GENERATOR.getAndIncrement();

        public ConsumingRunnable(BlockingQueue<Object> queue) {
            this.queue = queue;
        }

        @Override
        public void run() {
            try {
                for (;;) {
                    Object o = queue.take();
                    System.out.println("Fiber " + id + " consumed: " + o);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
    }


    public static class PoliteRunnable implements Runnable {
        private static final AtomicInteger ID_GENERATOR = new AtomicInteger();
        private static final long MAX_RUNNING_TIME_NANOS = MILLISECONDS.toNanos(50);

        private long id = ID_GENERATOR.getAndIncrement();
        private final BlockingQueue<Fiber> pausedFiberQueue;

        public PoliteRunnable(BlockingQueue<Fiber> pausedFiberQueue) {
            this.pausedFiberQueue = pausedFiberQueue;
        }

        public void run() {
            long lastParked = System.nanoTime();
            Fiber thisFiber = (Fiber) Strand.currentStrand();
            for (long counter = 0; ; counter++) {
                if (counter % 100000000 == 0) {
                    System.out.println("Fiber no. " + id + " at " + counter);
                }
                long now = System.nanoTime();
                long consumedTime = now - lastParked;
                if (consumedTime > MAX_RUNNING_TIME_NANOS) {
                    lastParked = now;
                    pausedFiberQueue.add(thisFiber);
                    Thread threadBeforeParking = Thread.currentThread();
                    Fiber.park();
                    Thread threadAfterParking = Thread.currentThread();
                    if (threadBeforeParking != threadAfterParking) {
                        System.out.println("Whooo, OS thread changed under feet! was: "
                                + threadBeforeParking + " now is: " + threadAfterParking);
                    }
                }
            }
        }
    }
}

