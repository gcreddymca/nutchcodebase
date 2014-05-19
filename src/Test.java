import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Test {

	public static void main(String args[]){
		Pattern PROTOCOLS = Pattern.compile("ftp");
		Matcher match = PROTOCOLS.matcher("ftp://localhost:8080/nrs.jsp");
		System.out.println(match.find());
	}
}
