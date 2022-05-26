package com.singlestore.s2dbdemo;

public class OutputTimeKeeper extends Thread {

	private long milliseconds = 0;
	private String timeDelim = "\n";
	
	public OutputTimeKeeper(long ms, String delim) {
		// TODO Auto-generated constructor stub
		this.milliseconds = ms;
		this.timeDelim = delim;
	}
	
	public void run() {

		try {
			while (true) {
				System.out.print(timeDelim);
				Thread.sleep(milliseconds);
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
		}	
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception  
	{  
	
		Thread timerThread = new OutputTimeKeeper(1000,"timer\n");
		timerThread.start();
		
		Thread.sleep(10000);
		
		timerThread.interrupt();
		System.out.println("Stopped");
	}
	
}
