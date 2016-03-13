package com.gsu.LogReader;

import kafka.utils.ZkUtils;

public class ResetOffset {
	public static void main(String[] args) {
		ZkUtils.maybeDeletePath("localhost:2181", "/consumers/group-0");
	}
}
