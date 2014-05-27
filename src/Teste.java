import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Teste {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String url = "http://localhost:8080/nrs/myaccount/login.jsp?error=";
//	 String[] exclusionList = { "error", "_dynSessConf",
//			"jsessionid", "_D", "_DARGS",
//			"/atg/store/profile/SessionBean.values.loginSuccessURL",
//			"categoryNavIds", "categoryNav", "navAction", "#", "loginFromHeader", "navCount", "resetFormErrors" };
//		for (String key : exclusionList) {
//			url = url.replaceAll("[;#&]*" + key
//					+ "[\\w%/.]*[=\\w-/.+%]*[$&]*", "");
//		}
//		

	//	url = url.replaceAll("[;]?[/\\w\\.]+=[\\w\\./]+", "");
	//	System.out.println("khjh" + url);
 

//Matcher m = p.matcher(url);
//while(m.find()){
//	System.out.println(m.group());
//}
		
	Pattern	p = Pattern.compile("[;&]*error[\\w%/.]*[=\\w-/.+%]*[$&]*");
	Matcher m = p.matcher(url);
	if(m.find()){
		System.out.println(url.replace(m.group(), ""));
	}
	}

}
