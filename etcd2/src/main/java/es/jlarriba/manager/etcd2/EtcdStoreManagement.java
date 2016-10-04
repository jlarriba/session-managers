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

import javax.management.MXBean;

/**
 * Management interface for the {@link com.gopivotal.manager.redis.RedisStore}
 */
@MXBean
public interface EtcdStoreManagement {

    /**
     * Returns the Etcd2 connection host
     *
     * @return the Etcd2 connection host
     */
    String getHost();

    /**
     * Returns the Etcd2 connection port
     *
     * @return the Etcd2 connection port
     */
    int getPort();
}
