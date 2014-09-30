package org.apache.nutch.util;


public class StatusCodeMessages {
	
	
public String getStatusMessage(int code){
	String message = null;
	
	switch (code) {

					case 2:
							message = "Content was not retrieved";
							break;
					case 10:
							message = "Protocol was not found";
							break;
					case 11:
							message = "Resource is gone";
							break;
					case 12:
							message = "Moved Permanently";
							break;
					case 13:
							message = "Moved Temporarily";
							break;
					case 14:
							message = "Page Not Found";
							break;
					case 15:
							message = "Temporary failure";
							break;
					case 16:
							message = "Internal Server Error";
							break;
					case 17:
							message = "Access denied";
							break;
					case 18:
							message = "Access denied by robots.txt rules";
							break;
					case 19:
							message = "Too many redirects";
							break;
					case 20:
							message = "Not fetching";
							break;
					case 21:
							message = "Unchanged since the last fetch";
							break;
					case 22:
							message = "Request refused by protocol plugins";
							break;
					case 23:
							message = "Thread was blocked";
							break;
					
					default:
							break;
}
	return message;
}
}
