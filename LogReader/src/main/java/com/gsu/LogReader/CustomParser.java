package com.gsu.LogReader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class CustomParser {
	public static void main(String[] args) {
		BufferedReader br = null;

		try {

			br = new BufferedReader(new FileReader("E:\\BetterHack\\LogFile\\log0.txt"));
			String str = br.readLine().trim();
																																									     
			int customerIdIndex = str.indexOf("employeeId");
			int customerIdIndexEnd = str.indexOf('"', customerIdIndex + 14);
			String customerId = str.substring(customerIdIndex+13, customerIdIndexEnd);
			System.out.println(customerId);
			
			
			int latIdIndex = str.indexOf("lat");
			int latIdIndexEnd = str.indexOf(',', latIdIndex + 4);
			String latValue = str.substring(latIdIndex+5, latIdIndexEnd);
			System.out.println(latValue);
			
			
			int lonIdIndex = str.indexOf("long");
			int lonIdIndexEnd = str.indexOf(',', lonIdIndex + 5);
			String lonValue = str.substring(lonIdIndex+6, lonIdIndexEnd);
			System.out.println(lonValue);
			
			int timeIdIndex = str.indexOf("time");
			String timeValue = str.substring(timeIdIndex+7, str.length()-2);
			System.out.println(timeValue);
			
			int eventIdIndex = str.indexOf("id");
			int eventIdIndexEnd = str.indexOf('"', eventIdIndex + 5);
			String eventId = str.substring(eventIdIndex+5, eventIdIndexEnd);
			System.out.println(eventId);
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

	}
}
