package net.mycache.core;

import java.util.Arrays;

public class ServerRuntimeInfo {
	private volatile boolean available;
	private final static int MAX_FAILED_TIMES = 10;
	private final static int INTERVAL_TIME = 1;//unit is second
	private final static int INIT_FAILED_TIME = 0;
	private long [] failedInfoSet;
	private int cursor;
	private long intervalTime;
	
	public ServerRuntimeInfo() {
		this(INTERVAL_TIME * 1000, MAX_FAILED_TIMES);
	}
	
	private void initFailedInfo() {
		for (int i = 0; i < failedInfoSet.length; i++) {
			failedInfoSet[i] = 0;
		}
		this.cursor = 0;	
	}
	
	public ServerRuntimeInfo(int intervalTime,int maxFailedTimes) {
		this.available = true;
		this.failedInfoSet = new long[maxFailedTimes];
		this.intervalTime = intervalTime;
		initFailedInfo();
	}
	
	public void incrFailedTimes() {
		failedInfoSet[cursor] = System.currentTimeMillis();
		cursor = (cursor + 1) % MAX_FAILED_TIMES;
	}
	
	public boolean isNeedDetect() {
		int lastPos = (cursor + MAX_FAILED_TIMES - 1) % MAX_FAILED_TIMES;
		if ((failedInfoSet[lastPos] != INIT_FAILED_TIME) && (failedInfoSet[cursor] != INIT_FAILED_TIME)) {
			if (failedInfoSet[lastPos] - failedInfoSet[cursor] >= intervalTime) {
				return true;
			}
		}
		return false;
	}
	
	public void setUnavailable() {
		this.available = false;
	}
	
	public boolean isAvailable() {
		return this.available;
	}
	
	public void reset() {
		this.available = true;
		initFailedInfo();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
    	builder.append("{");
		builder.append("available:");
		builder.append(available);
		builder.append(",failedInfoSet:");
		builder.append(Arrays.toString(failedInfoSet));
		builder.append(",cursor:");
		builder.append(cursor);
    	builder.append("}");
		return builder.toString();
	}
}
