package com.song.util;

/**
 * Created by Duo Nuo on 2018/2/7 0007.
 */
public class StringUtils {

	public static boolean isBlank(CharSequence cs) {
		int strLen;
		if(cs != null && (strLen = cs.length()) != 0) {
			for(int i = 0; i < strLen; ++i) {
				if(!Character.isWhitespace(cs.charAt(i))) {
					return false;
				}
			}

			return true;
		} else {
			return true;
		}
	}

	public static boolean isEmpty(CharSequence cs) {
		return cs == null || cs.length() == 0;
	}
}
