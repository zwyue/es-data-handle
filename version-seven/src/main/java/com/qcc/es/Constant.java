package com.qcc.es;

import java.util.ArrayList;
import java.util.List;

public interface Constant {

    /**
     * Quartz 常量
     */
    String QUARTZ_JOB_GROUP_FLINK_SAVE_POINT = "QUARTZ_JOB_FLINK_SAVE_POINT";
    String QUARTZ_JOB_TRIGGER_GROUP_FLINK_SAVE_POINT = "QUARTZ_JOB_TRIGGER_FLINK_SAVE_POINT";
    String QUARTZ_JOB_DATA_KEY = "QUARTZ_JOB_DATA_KEY";


    List<String> ids = new ArrayList<>() ;

}
