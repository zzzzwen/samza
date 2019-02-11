package org.apache.samza.clustermanager;

public class DMRMIListener implements DMListener {
    @Override
    public void registerToDM() {

    }

    @Override
    public void startListener() {
        DMRMIListenerServer server = new DMRMIListenerServer();
        Thread thread = new Thread(server);
//        thread.setDaemon(true);
        thread.start();
    }
}
