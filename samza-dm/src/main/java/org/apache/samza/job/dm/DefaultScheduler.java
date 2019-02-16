package org.apache.samza.job.dm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.samza.config.Config;
import org.apache.samza.config.DMDispatcherConfig;
import org.apache.samza.config.DMSchedulerConfig;

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
    	//TODO: create listener to listen to the update of resource and workload from monitor
    }

    @Override
    public void submitApplication() {
        LOG.info("scheduler submit application");
        // Use default schema to launch the application
        Allocation defaultAllocation = getDefaultAllocation("stage0");
        dispatcher.submitApplication(defaultAllocation);


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
}
