package com.orange.kafka;

/**
 * Created by Meirkhan Rakhmetzhanov on 25/6/19.
 */
class VersionUtil {
  public static String getVersion() {
    try {
      return VersionUtil.class.getPackage().getImplementationVersion();
    } catch(Exception ex){
      return "1.0.0";
    }
  }
}
