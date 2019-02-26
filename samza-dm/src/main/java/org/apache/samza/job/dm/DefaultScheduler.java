package org.apache.samza.job.dm;

import org.apache.commons.logging.Log;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.samza.config.Config;
import org.apache.samza.config.DMDispatcherConfig;
import org.apache.samza.config.DMSchedulerConfig;
import org.apache.samza.job.ApplicationStatus;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

public class DefaultScheduler implements DMScheduler {
    private static final Logger LOG = Logger.getLogger(DefaultScheduler.class.getName());

    private Config config;
    private DMSchedulerConfig schedulerConfig;

    private ConcurrentMap<String, Stage> stages;

    private DMDispatcher dispatcher;

    @Override
    public void init(Config config, DMSchedulerConfig schedulerConfig) {
        this.config = config;
        this.schedulerConfig = schedulerConfig;

        this.stages = new ConcurrentSkipListMap<>();

        DMDispatcherConfig dispatcherConfig = new DMDispatcherConfig(config);
        this.dispatcher = getDispatcher(dispatcherConfig.getDispatcherClass());
        this.dispatcher.init(config);
    }

    @Override
    public Allocation allocate(Resource clusterResource) {
        return null;
    }

    @Override
    public Allocation getDefaultAllocation(String stageId) {
        return new Allocation(stageId);
    }

    @Override
    public void createListener(DMScheduler scheduler) {
        LOG.info("starting listener in scheduler");
        String listenerClass = this.schedulerConfig.getSchedulerListenerClass();
        try {
            DMSchedulerListener listener = (DMSchedulerListener) Class.forName(listenerClass).newInstance();
            listener.setScheduler(this);
            listener.setConfig(config);
            listener.startListener();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void submitApplication() {
        LOG.info("scheduler submit application");
        // Use default schema to launch the application
        Allocation defaultAllocation = getDefaultAllocation(config.get("job.name"));
        dispatcher.submitApplication(defaultAllocation);

        // for kafka listener
        createListener(this);
        while (true) {}
    }

    @Override
    public DMDispatcher getDispatcher(String DMDispatcherClass) {
        LOG.info("scheduler getdispatcher");
        DMDispatcher dispatcher = null;
        try {
            dispatcher = (DMDispatcher) Class.forName(DMDispatcherClass).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return dispatcher;
    }

    @Override
    public void dispatch(Allocation allocation) {
        dispatcher.enforceSchema(allocation);
    }

    @Override
    public void updateStage(StageReport report) {
        // dataset can be in the format of String, JSON, XML, currently use String tentatively

        // update the url and port of listener to dispatcher at the first start up
        if (report.getType().equals("ApplicationMaster")) {
            if (!stages.containsKey(report.getName())) {
                LOG.info("creating new stage for application master");
                stages.put(report.getName(), new Stage());
            }
            Stage curr = stages.get(report.getName());
            curr.setRunningContainers(report.getRunningContainers());
            this.dispatcher.updateEnforcerURL(report.getName(), report.getHost()+ ":1999");
        } else if (report.getType().contains("TaskName-Partition")) {
            Stage curr = stages.get(report.getName());
            if (report.getThroughput() > 5 && curr.getRunningContainers() == 1) {
                LOG.info("Requesting scaling of containers");
//                this.dispatcher.enforceSchema(new Allocation(report.getName(), stages.get(report.getName()).getRunningContainers() +1));
                this.dispatcher.enforceSchema(new Allocation(report.getName(), 1));
            }

        }

         // TODO: add mechanism to trigger scheduling
    }
}
