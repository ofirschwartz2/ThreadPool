package bgu.spl.a2;

import java.util.Map;

/**
 * this class represents a single work stealing processor, it is
 * {@link Runnable} so it is suitable to be executed by threads.
 *
 * Note for implementors: you may add methods and synchronize any of the
 * existing methods in this class *BUT* you must be able to explain why the
 * synchronization is needed. In addition, the methods you add can only be
 * private, protected or package protected - in other words, no new public
 * methods
 *
 */
public class Processor implements Runnable {

	private final WorkStealingThreadPool pool;
	private final int id;
	private Map<Task<?>, Runnable> watingTasks;
	private boolean shouldStop=false;

	/**
	 * constructor for this class
	 *
	 * IMPORTANT:
	 * 1) this method is package protected, i.e., only classes inside
	 * the same package can access it - you should *not* change it to
	 * public/private/protected
	 *
	 * 2) you may not add other constructors to this class
	 * nor you allowed to add any other parameter to this constructor - changing
	 * this may cause automatic tests to fail..
	 *
	 * @param id - the processor id (every processor need to have its own unique
	 * id inside its thread pool)
	 * @param pool - the thread pool which owns this processor
	 */
	/*package*/ Processor(int id, WorkStealingThreadPool pool) {
		this.id = id;
		this.pool = pool;
	}

	@Override
	public void run() {
		while(!shouldStop) {
			int ver = pool.getVMVersion();
			Task<?> nextTask = pool.getNextTask(id);
			if (nextTask == null) {
				nextTask=pool.stealTasks(id, ver);
				if(nextTask != null) {
				//	System.out.println("processor "+id+" received a task!	"+nextTask.toString());
					nextTask.handle(this);
				}
				//nextTask.handle(this);
			}
			else {
			//	System.out.println("processor "+id+" received a task!	"+nextTask.toString());
				nextTask.handle(this);	
			}
		}
	}

	/**
	 * 
	 * @return the processor id
	 */
	/*package*/ int getId() {
		return id;
	}
	
	/**
	 * 
	 * @param t the task to be added to the waiting-tasks queue
	 * @param callback the callback to be executed when task t exits the waiting-tasks queue
	 */
	/*package*/ void addToWaitingTasks(Task<?> t,Runnable callback) {
		if (!watingTasks.containsKey(t))
			watingTasks.put(t, callback);
	}
	
	/**
	 * 
	 * @return this processor pool
	 */
	/*package*/ WorkStealingThreadPool getPool() {
		return pool;
	}
	
	/**
	 * inform this processor to stop working
	 */
	/*package*/ void stopProcessor() {
		shouldStop=true;
	}
}
