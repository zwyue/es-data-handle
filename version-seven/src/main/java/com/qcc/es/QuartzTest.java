package com.qcc.es;

import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class QuartzTest {

    public static void main(String[] args) {

        try {

            // Grab the Scheduler instance from the Factory
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();


            for (int i = 0; i < 120; i++) {
                // create job
                JobDetail
                    jobDetail = JobBuilder.newJob(ExecuteQuartzJob.class).withIdentity(i + "",
                    Constant.QUARTZ_JOB_GROUP_FLINK_SAVE_POINT).build();
                jobDetail.getJobDataMap().put(Constant.QUARTZ_JOB_DATA_KEY, i + "");
                // create plan
                CronScheduleBuilder scheduleBuilder =
                    CronScheduleBuilder.cronSchedule("0/10 * * * * ?")
                        .withMisfireHandlingInstructionDoNothing();
                // create trigger and bind plan
                CronTrigger trigger = TriggerBuilder.newTrigger()
                    .withIdentity(i + "", Constant.QUARTZ_JOB_TRIGGER_GROUP_FLINK_SAVE_POINT)
                    .withSchedule(scheduleBuilder).build();
                scheduler.scheduleJob(jobDetail, trigger);
            }

            // and start it off
            scheduler.start();

//            scheduler.shutdown();

        } catch (SchedulerException se) {
            se.printStackTrace();
        }
    }
}
