/**
 * The Log class allows operators to log relevant data for post-execution analysis.
 * 
 * This initial version assumes maintaining string key-value pairs.
 */
package niagara.utils;

import java.util.*;

/**
 * @author rfernand
 * @version 1.0 
 * 
 */
public class Log {

	private Hashtable<String, String> _log;
	private String _operatorName;

	@SuppressWarnings("unchecked")
	public Log Copy(){
		Log copy = new Log(this._operatorName);
		copy._log = (Hashtable<String, String>) this._log.clone();
		return copy;
	}
	
	public Log(String operatorName){
		_operatorName = operatorName;
		_log = new Hashtable<String, String>();
	}

	public void Update(String key, String value) {
		if(_log.get(key) != null) {
			_log.remove(key);
			_log.put(key,value);
		} else {
			_log.put(key,value);
		}
	}

	public String ToString() {

		Enumeration<String> keys = _log.keys();
		String key;

		String _message = "";
		
		/*_message+="<"+ _operatorName + ">\n";
		
		while(keys.hasMoreElements())
		{
			key = (String)keys.nextElement();
			_message += "<record><key>"+key+"</key><value>"+_log.get(key)+"</value></record>\n";
		}

		_message += "</" + _operatorName + ">\n";*/
		
		while(keys.hasMoreElements())
		{
			key = (String)keys.nextElement();
			_message += ""+key+","+_log.get(key)+"\n";
		}
	
		return _message;
	}
}
