package th.co.ais.cpac.cl.batch.db.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DaoUtility {
	public static boolean isNull(String value){
		if(value==null||"".equals(value)){
			return true;
		}else{
			return false;
		}
	}
	public static boolean isEqual(String a,String b){
		if(a.equals(b)){
			return true;
		}else{
			return false;
		}
	}
	public static String convertDateToString(Date dateTime,String format) {
		DateFormat simpleFormat = new SimpleDateFormat(format);
		String dateTimeString = simpleFormat.format(dateTime);
		return dateTimeString;
	}
}
