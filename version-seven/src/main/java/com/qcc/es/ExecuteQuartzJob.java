package com.qcc.es;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

public class ExecuteQuartzJob implements Job {

    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws JobExecutionException {

        String jobId =
            (String) jobExecutionContext.getMergedJobDataMap().get(Constant.QUARTZ_JOB_DATA_KEY);

        Integer ids1 = 0 ;
        for (int i = 0; i < 1000; i++) {
            ids1 ++ ;
            for (int j = 0; j < 1000; j++) {
                ids1 ++ ;
                for (int k = 0; k < 2000; k++) {
                    ids1 ++ ;
                }
            }
        }
        System.out.println(jobId);
        System.out.println(ids1);
    }
}
