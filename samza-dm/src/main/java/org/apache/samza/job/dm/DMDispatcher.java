package org.apache.samza.job.dm;


import org.apache.samza.config.Config;

public interface DMDispatcher {

    void init(Config config);
    /**
     *  Create new enforcer when the stage is initiated
     *
     * @param fac  the factory object to generate enforcer
     * @return the enforcer created
     */
    EnforcerFactory getEnforcerFactory(String stage);

    /**
     *  Create new enforcer when the stage is initiated
     *
     * @param stageId  the factory object to generate enforcer
     * @return the enforcer created
     */
    Enforcer getEnforcer(String stageId);


    /**
     * enforce the allocation for the specific stage
     *
     * @param allocation
     */
    void enforceSchema(Allocation allocation);


    /**
     * submit the application to the cluster with default allocation
     */
    void submitApplication(Allocation allocation);

}
