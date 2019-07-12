package bgu.spl.a2;


import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * represents a work stealing thread pool - to understand what this class does
 * please refer to your assignment.
 *
 * Note for implementors: you may add methods and synchronize any of the
 * existing methods in this class *BUT* you must be able to explain why the
 * synchronization is needed. In addition, the methods you add can only be
 * private, protected or package protected - in other words, no new public
 * methods
 */
public class WorkStealingThreadPool {


	private VersionMonitor vm = new VersionMonitor();
	private ConcurrentHashMap <Integer,ConcurrentLinkedDeque<Task<?>>> processorsQueues;	
	private HashMap<Integer, Processor> processors;
	private HashMap<Integer, Thread> threads;

	/**
	 * creates a {@link WorkStealingThreadPool} which has nthreads
	 * {@link Processor}s. Note, threads should not get started until calling to
	 * the {@link #start()} method.
	 *
	 * Implementors note: you may not add other constructors to this class nor
	 * you allowed to add any other parameter to this constructor - changing
	 * this may cause automatic tests to fail..
	 *
	 * @param nthreads the number of threads that should be started by this
	 * thread pool
	 */
	public WorkStealingThreadPool(int nthreads) {
		processorsQueues=new ConcurrentHashMap<Integer, ConcurrentLinkedDeque<Task<?>>>();
		for (int i=0; i<nthreads; i++) {
			processorsQueues.put(new Integer(i), new ConcurrentLinkedDeque<Task<?>>());
		}
	}

	/**
	 * submits a task to be executed by a processor belongs to this thread pool
	 *
	 * @param task the task to execute
	 */
	public void submit(Task<?> task) {
		Random rnd=new Random();
		processorsQueues.get((rnd.nextInt(processorsQueues.size()))).addFirst(task);
		vm.inc();
	}
	
	/**
	 * 
	 * @param t the task to be submitted
	 * @param processorId the processor id to be given the task
	 */
	/*package*/ void submit(Task<?> t, int processorId) {
		processorsQueues.get(processorId).addFirst(t);
		vm.inc();
	}

	/**
	 * closes the thread pool - this method interrupts all the threads and wait
	 * for them to stop - it is returns *only* when there are no live threads in
	 * the queue.
	 *
	 * after calling this method - one should not use the queue anymore.
	 *        
	 * @throws InterruptedException if the thread that shut down the threads is
	 * interrupted
	 * @throws UnsupportedOperationException if the thread that attempts to
	 * shutdown the queue is itself a processor of this queue
	 */
	public void shutdown() throws InterruptedException {
		for(Integer i=new Integer(0); i.intValue()<processors.size(); i++) {
			processors.get(i).stopProcessor();
		}
		vm.inc();
		for(Integer i=new Integer(0); i.intValue()<processors.size(); i++) {
			threads.get(i).join();
		}
	}

	/**
	 * start the threads belongs to this thread pool
	 */
	public void start() {
		processors=new HashMap<Integer, Processor>();
		threads=new HashMap<Integer, Thread>();
		for (int i=0; i<processorsQueues.size(); i++) {
			Processor p = new Processor(i, this);
			processors.put(new Integer(i), p);
			Thread th = new Thread(p);
			threads.put(new Integer(i), th);
			th.start();
		}
	}

	/*package*/ Task<?> getNextTask(int id) {
		return processorsQueues.get(id).pollFirst();
	}

	/*package*/ Task<?> stealTasks(int id, int ver) {
		AtomicInteger index = new AtomicInteger(id+1);
		while(true) {
			if (index.get() == processorsQueues.size()) index.set(0);
			if (index.get() == id) {
				try {
					if (ver != vm.getVersion()) {
						Task<?> t=processorsQueues.get(id).pollFirst();
						if (t != null) return t;
					}
					vm.await(vm.getVersion());
					return processorsQueues.get(id).pollFirst();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			else {
				boolean stole=false;
				int size=processorsQueues.get(index.get()).size();
				if (size > 1) {
					for (int i=0; i < (size/2); i++) {
						Task<?> t = processorsQueues.get(index.get()).pollLast();
						if (t != null) {
							processorsQueues.get(id).addFirst(t);
							stole=true;
						}
						else break;
					}
					if (stole) return processorsQueues.get(id).pollFirst();
				}
			}
			index.incrementAndGet();
		}
	}
	
	/*package*/int getVMVersion() {
		return this.vm.getVersion();
	}

}
