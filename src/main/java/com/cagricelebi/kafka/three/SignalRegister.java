package com.cagricelebi.kafka.three;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import sun.misc.Signal;
import sun.misc.SignalHandler;

/**
 *
 * @author cagricelebi
 */
public class SignalRegister {

    private final Map<String, List<Listener>> listenerMap = new ConcurrentHashMap<>();

    public static interface Listener {

        public void handleSignal();
    }

    public synchronized static void attach(String[] signalNames, Listener listener) {
        try {
            for (final String signalName : signalNames) {
                new SignalHandler() {
                    private SignalHandler old = Signal.handle(new Signal(signalName), this);

                    @Override
                    public void handle(Signal sig) {
                        notifyListeners(sig.getName());
                        //if (old != SignalHandler.SIG_DFL && old != SignalHandler.SIG_IGN) {
                        //    old.handle(sig);
                        //}
                    }
                };
                List<Listener> listenerList = getInstance().listenerMap.get(signalName);
                if (listenerList == null) {
                    listenerList = new ArrayList<>();
                }
                listenerList.add(listener);
                getInstance().listenerMap.put(signalName, listenerList);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private synchronized static void notifyListeners(String trigger) {
        for (Map.Entry<String, List<Listener>> entry : getInstance().listenerMap.entrySet()) {
            String registeredSignalName = entry.getKey();
            if (registeredSignalName.contentEquals(trigger)) {
                List<Listener> registeredListeners = entry.getValue();
                for (Listener listener : registeredListeners) {
                    try {
                        listener.handleSignal();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }

    // <editor-fold defaultstate="collapsed" desc="Singleton structure.">
    private SignalRegister() {
    }

    private static class SignalRegisterInstanceHolder {

        private static volatile SignalRegister instance = new SignalRegister();
    }

    private synchronized static SignalRegister getInstance() {
        return SignalRegisterInstanceHolder.instance;
    }
    // </editor-fold>

}
