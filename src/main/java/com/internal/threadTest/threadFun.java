package com.internal.threadTest;

public class threadFun {
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		int n = 10;
		if (n < 2)
			return;
		Object[] lockObjects = new Object[n];
		for (int i = 0; i < n; i++) {
			lockObjects[i] = new Object();
		}

		for (int i = 0; i < n; i++) {
			new NumberThread("Thread-" + (i + 1), lockObjects[i],
					lockObjects[(i + 1) % n]).start();
		}

	}

}

class NumberThread extends Thread {
	Object thisLock;
	Object nextLock;
	final String threadName;

	public NumberThread(String threadName, Object thisLock, Object nextLock) {
		this.nextLock = nextLock;
		this.thisLock = thisLock;
		this.threadName = threadName;
	}

	public void run() {
		for (int i = 0; i < 500; i++) {
			synchronized (thisLock) {
//				synchronized (nextLock) {
//					nextLock.notify();

//					try {
//						Thread.sleep(2);
//					} catch (InterruptedException e) {
//						// TODO Auto-generated catch block
//						e.printStackTrace();
//					}
					System.out.println(threadName + " " + Number.getNumber());

//				}
//				try {
//					thisLock.wait();
//				} catch (InterruptedException e) {
//					// TODO Auto-generated catch block
//					e.printStackTrace();
//				}
			}
		}
	}
}

class Number {
	static int i = 0;

	public static int getNumber() {
		return ++i;
	}
}