package org.apache.samza.job.dm;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.samza.config.Config;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Logger;

public class DefaultScheduler implements DMScheduler {
    private static final Logger LOG = Logger.getLogger(DefaultScheduler.class.getName());

    private Config config;
    private ConcurrentMap<String, Stage> stages;
    private DMDispatcher dispatcher;

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
    public void init(Config config) {
        this.config = config;
        this.stages = new ConcurrentSkipListMap<>();
        this.dispatcher = getDispatcher();
        this.dispatcher.init(config);
    }

    @Override
    public void submitApplication() {
        LOG.info("scheduler submit application");
        // Use default schema to launch the application
        Allocation defaultAllocation = getDefaultAllocation("stage0");
        dispatcher.submitApplication(defaultAllocation);
    }

    @Override
    public DMDispatcher getDispatcher() {
        LOG.info("scheduler getdispatcher");
        String DMDispatcherClass = "DefaultDispatcher";
        if (config.containsKey("dm.dispatcher.class")) {
            DMDispatcherClass = config.get("dm.dispatcher.class", "DefaultDispatcher");
        }
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
