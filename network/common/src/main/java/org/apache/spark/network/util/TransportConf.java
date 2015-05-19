/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.util;

import com.google.common.primitives.Ints;

import java.io.File;

/**
 * A central location that tracks all the settings we expose to users.
 */
public class TransportConf {
  private final ConfigProvider conf;

  public TransportConf(ConfigProvider conf) {
    this.conf = conf;
  }

  /**
   * IO mode: nio or epoll
   */
  public String ioMode() {
    return conf.get("spark.shuffle.io.mode", "NIO").toUpperCase();
  }

  /**
   * If true, we will prefer allocating off-heap byte buffers within Netty.
   */
  public boolean preferDirectBufs() {
    return conf.getBoolean("spark.shuffle.io.preferDirectBufs", true);
  }

  /**
   * Connect timeout in milliseconds. Default 120 secs.
   */
  public int connectionTimeoutMs() {
    long defaultNetworkTimeoutS = JavaUtils.timeStringAsSec(
      conf.get("spark.network.timeout", "120s"));
    long defaultTimeoutMs = JavaUtils.timeStringAsSec(
      conf.get("spark.shuffle.io.connectionTimeout", defaultNetworkTimeoutS + "s")) * 1000;
    return (int) defaultTimeoutMs;
  }

  /**
   * Number of concurrent connections between two nodes for fetching data.
   */
  public int numConnectionsPerPeer() {
    return conf.getInt("spark.shuffle.io.numConnectionsPerPeer", 1);
  }

  /**
   * Requested maximum length of the queue of incoming connections. Default -1 for no backlog.
   */
  public int backLog() {
    return conf.getInt("spark.shuffle.io.backLog", -1);
  }

  /**
   * Number of threads used in the server thread pool. Default to 0, which is 2x#cores.
   */
  public int serverThreads() {
    return conf.getInt("spark.shuffle.io.serverThreads", 0);
  }

  /**
   * Number of threads used in the client thread pool. Default to 0, which is 2x#cores.
   */
  public int clientThreads() {
    return conf.getInt("spark.shuffle.io.clientThreads", 0);
  }

  /**
   * Receive buffer size (SO_RCVBUF).
   * Note: the optimal size for receive buffer and send buffer should be
   * latency * network_bandwidth.
   * Assuming latency = 1ms, network_bandwidth = 10Gbps
   * buffer size should be ~ 1.25MB
   */
  public int receiveBuf() {
    return conf.getInt("spark.shuffle.io.receiveBuffer", -1);
  }

  /**
   * Send buffer size (SO_SNDBUF).
   */
  public int sendBuf() {
    return conf.getInt("spark.shuffle.io.sendBuffer", -1);
  }

  /**
   * Timeout for a single round trip of SASL token exchange, in milliseconds.
   */
  public int saslRTTimeoutMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get("spark.shuffle.sasl.timeout", "30s")) * 1000;
  }

  /**
   * Max number of times we will try IO exceptions (such as connection timeouts) per request.
   * If set to 0, we will not do any retries.
   */
  public int maxIORetries() {
    return conf.getInt("spark.shuffle.io.maxRetries", 3);
  }

  /**
   * Time (in milliseconds) that we will wait in order to perform a retry after an IOException.
   * Only relevant if maxIORetries &gt; 0.
   */
  public int ioRetryWaitTimeMs() {
    return (int) JavaUtils.timeStringAsSec(conf.get("spark.shuffle.io.retryWait", "5s")) * 1000;
  }

  /**
   * Minimum size of a block that we should start using memory map rather than reading in through
   * normal IO operations. This prevents Spark from memory mapping very small blocks. In general,
   * memory mapping has high overhead for blocks close to or below the page size of the OS.
   */
  public int memoryMapBytes() {
    return conf.getInt("spark.storage.memoryMapThreshold", 2 * 1024 * 1024);
  }

  /**
   * Whether to initialize shuffle FileDescriptor lazily or not. If true, file descriptors are
   * created only when data is going to be transferred. This can reduce the number of open files.
   */
  public boolean lazyFileDescriptor() {
    return conf.getBoolean("spark.shuffle.io.lazyFD", true);
  }

  /**
   * When Secure (SSL/TLS) Shuffle is enabled, the Chunk size to use for shuffling files.
   *
   * @return
   */
  public int sslShuffleChunkSize() {
    return conf.getInt("spark.shuffle.io.ssl.chunkSize", 60 * 1024);
  }

  /**
   * Whether Secure (SSL/TLS) Shuffle (Block Transfer Service) is enabled
   *
   * @return
   */
  public boolean sslShuffleEnabled() {
    return conf.getBoolean("spark.ssl.bts.enabled", false);
  }

  /**
   * SSL protocol (remember that SSLv3 was compromised) supported by Java
   *
   * @return
   */
  public String sslShuffleProtocol() {
    return conf.get("spark.ssl.bts.protocol", null);
  }

  /**
   * A comma separated list of ciphers
   *
   * @return
   */
  public String[] sslShuffleRequestedCiphers() {
    String ciphers = conf.get("spark.ssl.bts.enabledAlgorithms", null);
    return (ciphers != null ? ciphers.split(",") : null);
  }

  /**
   * The key-store file; can be relative to the current directory
   *
   * @return
   */
  public File sslShuffleKeyStore() {
    String keyStore = conf.get("spark.ssl.bts.keyStore", null);
    if (keyStore != null)
      return new File(keyStore);
    else
      return null;
  }

  /**
   * The password to the key-store file
   *
   * @return
   */
  public String sslShuffleKeyStorePassword() {
    return conf.get("spark.ssl.bts.keyStorePassword", null);
  }

  /**
   * A PKCS#8 private key file in PEM format; can be relative to the current directory
   *
   * @return
   */
  public File sslShufflePrivateKey() {
    String privateKey = conf.get("spark.ssl.bts.privateKey", null);
    if (privateKey != null)
      return new File(privateKey);
    else
      return null;
  }

  /**
   * The password to the private key
   *
   * @return
   */
  public String sslShuffleKeyPassword() {
    return conf.get("spark.ssl.bts.keyPassword", null);
  }

  /**
   * A X.509 certificate chain file in PEM format; can be relative to the current directory
   *
   * @return
   */
  public File sslShuffleCertChain() {
    String certChain = conf.get("spark.ssl.bts.certChain", null);
    if (certChain != null)
      return new File(certChain);
    else
      return null;
  }

  /**
   * The trust-store file; can be relative to the current directory
   *
   * @return
   */
  public File sslShuffleTrustStore() {
    String trustStore = conf.get("spark.ssl.bts.trustStore", null);
    if (trustStore != null)
      return new File(trustStore);
    else
      return null;
  }

  /**
   * The password to the trust-store file
   *
   * @return
   */
  public String sslShuffleTrustStorePassword() {
    return conf.get("spark.ssl.bts.trustStorePassword", null);
  }

  /**
   * If using a trust-store that that reloads its configuration is enabled.
   * If true, when the trust-store file on disk changes, it will be reloaded
   *
   * @return
   */
  public boolean sslShuffleTrustStoreReloadingEnabled() {
    return conf.getBoolean("spark.ssl.bts.trustStoreReloadingEnabled", false);
  }

  /**
   * The interval, in milliseconds, the trust-store will reload its configuration
   *
   * @return
   */
  public int sslShuffleTrustStoreReloadInterval() {
    return conf.getInt("spark.ssl.bts.trustStoreReloadInterval", 10000);
  }

  /**
   * If the OpenSSL implementation is enabled,
   * (if available on host system), requires certChain and keyFile arguments
   *
   * @return
   */
  public boolean sslShuffleOpenSslEnabled() {
    return conf.getBoolean("spark.ssl.bts.openSslEnabled", false);
  }

  /**
   * Maximum number of retries when binding to a port before giving up.
   */
  public int portMaxRetries() {
    return conf.getInt("spark.port.maxRetries", 16);
  }

  /**
   * Maximum number of bytes to be encrypted at a time when SASL encryption is enabled.
   */
  public int maxSaslEncryptedBlockSize() {
    return Ints.checkedCast(JavaUtils.byteStringAsBytes(
      conf.get("spark.network.sasl.maxEncryptedBlockSize", "64k")));
  }

  /**
   * Whether the server should enforce encryption on SASL-authenticated connections.
   */
  public boolean saslServerAlwaysEncrypt() {
    return conf.getBoolean("spark.network.sasl.serverAlwaysEncrypt", false);
  }

}
