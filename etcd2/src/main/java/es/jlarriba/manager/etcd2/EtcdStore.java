/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.jlarriba.manager.etcd2;

import com.gopivotal.manager.AbstractLifecycle;
import com.gopivotal.manager.LockTemplate;
import com.gopivotal.manager.PropertyChangeSupport;
import com.gopivotal.manager.SessionFlushValve;
import com.gopivotal.manager.SessionSerializationUtils;
import com.gopivotal.manager.StandardPropertyChangeSupport;
import org.apache.catalina.Manager;
import org.apache.catalina.Session;
import org.apache.catalina.Store;
import org.apache.catalina.Valve;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.net.URI;
import java.util.Base64;
import java.util.List;
import java.util.logging.Logger;

import static java.util.logging.Level.SEVERE;
import org.boon.etcd.ClientBuilder;
import org.boon.etcd.Etcd;
import org.boon.etcd.Node;
import org.boon.etcd.Response;

/**
 * An implementation of {@link Store} that persists data to Etcd2
 */
public final class EtcdStore extends AbstractLifecycle implements EtcdStoreManagement, Store {
    
    private static final String INFO = "EtcdStore/1.0";
    private static final String SESSIONS_KEY = "sessions";
    private final LockTemplate lockTemplate = new LockTemplate();
    private final Logger logger = Logger.getLogger(this.getClass().getName());
    private final PropertyChangeSupport propertyChangeSupport;
    private volatile String host = "localhost";
    private volatile Etcd client;
    private volatile Manager manager;
    private volatile int port = 2379;
    private volatile SessionSerializationUtils sessionSerializationUtils;

    /**
     * Create a new instance
     *
     * @see com.gopivotal.manager.StandardPropertyChangeSupport
     */
    public EtcdStore() {
        this.logger.info(String.format("Sessions will be persisted to ETCD using a %s", this.getClass().getName()));
        this.propertyChangeSupport = new StandardPropertyChangeSupport(this);
    }

    EtcdStore(Etcd client, PropertyChangeSupport propertyChangeSupport,
            SessionSerializationUtils sessionSerializationUtils) {
        this.client = client;
        this.propertyChangeSupport = propertyChangeSupport;
        this.sessionSerializationUtils = sessionSerializationUtils;
    }
    
    @Override
    public String getInfo() {
        return INFO;
    }
    
    @Override
    public void addPropertyChangeListener(PropertyChangeListener propertyChangeListener) {
        this.propertyChangeSupport.add(propertyChangeListener);
    }

    @Override
    public void clear() {
        this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                Response response = client.deleteDirRecursively(SESSIONS_KEY);
                if (response == null || response.wasError()) {
                    EtcdStore.this.logger.log(SEVERE, "Unable to clear persisted sessions");
                }
                return null;
            }

        });
    }

    @Override
    public String getHost() {
        return this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<String>() {

            @Override
            public String invoke() {
                return EtcdStore.this.host;
            }

        });
    }

    /**
     * Sets the host to connect to
     *
     * @param host the host to connect to
     */
    public void setHost(final String host) {
        this.lockTemplate.withWriteLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                String previous = EtcdStore.this.host;
                EtcdStore.this.host = host;
                EtcdStore.this.propertyChangeSupport.notify("host", previous, EtcdStore.this.host);
                return null;
            }

        });
    }

    @Override
    public Manager getManager() {
        return this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<Manager>() {

            @Override
            public Manager invoke() {
                return EtcdStore.this.manager;
            }

        });
    }

    @Override
    public void setManager(final Manager manager) {
        this.lockTemplate.withWriteLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                Manager previous = EtcdStore.this.manager;
                EtcdStore.this.manager = manager;
                EtcdStore.this.sessionSerializationUtils = new SessionSerializationUtils(manager);
                EtcdStore.this.propertyChangeSupport.notify("manager", previous, EtcdStore.this.manager);
                return null;
            }

        });
    }

    @Override
    public int getPort() {
        return this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<Integer>() {

            @Override
            public Integer invoke() {
                return EtcdStore.this.port;
            }

        });
    }

    /**
     * Sets the port to connect to
     *
     * @param port the port to connect to
     */
    public void setPort(final int port) {
        this.lockTemplate.withWriteLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                int previous = EtcdStore.this.port;
                EtcdStore.this.port = port;
                EtcdStore.this.propertyChangeSupport.notify("port", previous, EtcdStore.this.port);
                return null;
            }

        });
    }

    @Override
    public int getSize() {
        return this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<Integer>() {

            @Override
            public Integer invoke() {
                Response response = client.list(SESSIONS_KEY);
                if (response != null && response.successful()) {
                    return response.node().getNodes().size();
                }

                return Integer.MIN_VALUE;
            }

        });
    }

    @Override
    public String[] keys() {
        return this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<String[]>() {

            @Override
            public String[] invoke() {
                
                Response response = client.list(SESSIONS_KEY);
                if (response != null && response.successful()) {
                    List<Node> nodes = response.node().getNodes();
                    return (String[]) nodes.toArray();
                }
                
                return null;
            }

        });
    }

    @Override
    public Session load(final String id) {
        return this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<Session>() {

            @Override
            public Session invoke() {
                Response response = client.get(getKey(id));
                if (response != null && response.successful()) {
                    try {
                        return EtcdStore.this.sessionSerializationUtils.deserialize(Base64.getDecoder().decode(response.node().getValue()));
                    } catch (ClassNotFoundException e) {
                        EtcdStore.this.logger.log(SEVERE, String.format("Unable to load session %s. Empty session created.", id), e);
                        return EtcdStore.this.manager.createSession(id);
                    } catch (IOException e) {
                        EtcdStore.this.logger.log(SEVERE, String.format("Unable to load session %s. Empty session created.", id), e);
                        return EtcdStore.this.manager.createSession(id);
                    }
                } else {
                    EtcdStore.this.logger.log(SEVERE, String.format("Unable to load session %s. Empty session created.", id));
                    return EtcdStore.this.manager.createSession(id);
                }
            }
        });
    }

    @Override
    public void remove(final String id) {
        this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                Response response = client.delete(id);
                if (response == null || response.wasError()) {
                    EtcdStore.this.logger.log(SEVERE, String.format("Unable to remove session %s", id));
                }

                return null;
            }

        });
    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener propertyChangeListener) {
        this.propertyChangeSupport.remove(propertyChangeListener);
    }

    @Override
    public void save(final Session session) {
        this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                final String id = session.getId();
                
                try {
                    Response response = client.set(getKey(id), Base64.getEncoder().encodeToString(EtcdStore.this.sessionSerializationUtils.serialize(session)));
                    if (response == null || response.wasError()) {
                        EtcdStore.this.logger.log(SEVERE, String.format("Unable to save session %s", id));
                    }
                } catch (IOException e) {
                    EtcdStore.this.logger.log(SEVERE, String.format("Unable to save session %s", id), e);
                }
                
                return null;
            }

        }
        );
    }

    @Override
    protected void initInternal() {
        this.lockTemplate.withReadLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                for (Valve valve : EtcdStore.this.manager.getContainer().getPipeline().getValves()) {
                    if (valve instanceof SessionFlushValve) {
                        EtcdStore.this.logger.fine(String.format("Setting '%s' as the store for '%s'", this, valve));
                        ((SessionFlushValve) valve).setStore(EtcdStore.this);
                    }
                }

                return null;
            }

        });
    }

    @Override
    protected void startInternal() {
        this.lockTemplate.withWriteLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                if (EtcdStore.this.client == null) {
                    EtcdStore.this.client = ClientBuilder.builder().hosts(URI.create("http://" + EtcdStore.this.host + ":" + EtcdStore.this.port)).createClient();
                }
                return null;
            }
        });
    }

    @Override
    protected void stopInternal() {
        this.lockTemplate.withWriteLock(new LockTemplate.LockedOperation<Void>() {

            @Override
            public Void invoke() {
                // The connection is not persistent, so nothing to do
                return null;
            }

        });
    }

    private String getKey(final String id) {
        return SESSIONS_KEY + "/" + id;
    }
}
