/*
 * Copyright 2021 Yelp Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yelp.nrtsearch.server.grpc;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Class for holding configuration information for channel building. Designed to be loadable using
 * jackson. Unless otherwise specified, all fields may be null to used the default channel values.
 */
@JsonInclude(Include.NON_NULL)
public class ChannelConfig {
  private Boolean enableRetry;
  private Integer maxHedgedAttempts;
  private Integer maxInboundMessageSize;
  private ServiceConfig serviceConfig;

  // Deserialization constructor
  public ChannelConfig() {}

  /**
   * Constructor.
   *
   * @param enableRetry if the retry system should be enabled
   * @param maxHedgedAttempts channel level max for hedge attempts
   * @param maxInboundMessageSize maximum size of inbound messages in bytes
   * @param serviceConfig additional service configuration
   */
  public ChannelConfig(
      Boolean enableRetry,
      Integer maxHedgedAttempts,
      Integer maxInboundMessageSize,
      ServiceConfig serviceConfig) {
    this.enableRetry = enableRetry;
    this.maxHedgedAttempts = maxHedgedAttempts;
    this.maxInboundMessageSize = maxInboundMessageSize;
    this.serviceConfig = serviceConfig;
  }

  public Boolean getEnableRetry() {
    return enableRetry;
  }

  public Integer getMaxHedgedAttempts() {
    return maxHedgedAttempts;
  }

  public Integer getMaxInboundMessageSize() {
    return maxInboundMessageSize;
  }

  public ServiceConfig getServiceConfig() {
    return serviceConfig;
  }

  /**
   * Set properties on channel builder based on configuration.
   *
   * @param builder input channel builder
   * @param mapper object mapper
   * @return builder with properties set
   */
  public ManagedChannelBuilder<?> configureChannelBuilder(
      ManagedChannelBuilder<?> builder, ObjectMapper mapper) {
    if (enableRetry != null) {
      if (enableRetry) {
        builder.enableRetry();
      } else {
        builder.disableRetry();
      }
    }
    if (maxHedgedAttempts != null) {
      builder.maxHedgedAttempts(maxHedgedAttempts);
    }
    if (maxInboundMessageSize != null) {
      builder.maxInboundMessageSize(maxInboundMessageSize);
    }
    if (serviceConfig != null) {
      @SuppressWarnings("unchecked")
      Map<String, ?> defaultServiceConfig = mapper.convertValue(serviceConfig, Map.class);
      builder.defaultServiceConfig(defaultServiceConfig);
    }
    return builder;
  }

  /**
   * Additional service configuration properties. Corresponding to the <a
   * href="https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto">protobuf</a>
   * specification. Types must conform to the <a
   * href="https://developers.google.com/protocol-buffers/docs/proto3#json">json encoded</a>
   * message. Note that this requires all number values to be doubles.
   */
  @JsonInclude(Include.NON_NULL)
  public static class ServiceConfig {
    private List<MethodConfig> methodConfig;
    private RetryThrottlingConfig retryThrottling;

    // Deserialization constructor
    public ServiceConfig() {}

    /**
     * Constructor.
     *
     * @param methodConfig per method configuration
     * @param retryThrottling retry throttling configuration
     */
    public ServiceConfig(List<MethodConfig> methodConfig, RetryThrottlingConfig retryThrottling) {
      this.methodConfig = methodConfig;
      this.retryThrottling = retryThrottling;
    }

    public List<MethodConfig> getMethodConfig() {
      return methodConfig;
    }

    public RetryThrottlingConfig getRetryThrottling() {
      return retryThrottling;
    }

    /** Method configuration. */
    @JsonInclude(Include.NON_NULL)
    public static class MethodConfig {
      private List<MethodName> name;
      private String timeout;
      private HedgingPolicy hedgingPolicy;

      // Deserialization constructor
      public MethodConfig() {}

      /**
       * Constructor.
       *
       * @param name method names, this must be specified and contain at least one value
       * @param timeout method deadline
       * @param hedgingPolicy hedging config
       * @throws NullPointerException if name is null
       * @throws IllegalArgumentException if name is empty
       */
      public MethodConfig(List<MethodName> name, String timeout, HedgingPolicy hedgingPolicy) {
        Objects.requireNonNull(name);
        if (name.isEmpty()) {
          throw new IllegalArgumentException("At least one method name must be specified");
        }
        this.name = name;
        this.timeout = timeout;
        this.hedgingPolicy = hedgingPolicy;
      }

      public List<MethodName> getName() {
        return name;
      }

      public String getTimeout() {
        return timeout;
      }

      public HedgingPolicy getHedgingPolicy() {
        return hedgingPolicy;
      }

      /**
       * Request hedging configuration. See <a
       * href="https://github.com/grpc/proposal/blob/master/A6-client-retries.md#hedging-policy-1">docs</a>.
       */
      @JsonInclude(Include.NON_NULL)
      public static class HedgingPolicy {
        private Double maxAttempts;
        private String hedgingDelay;
        private List<String> nonFatalStatusCodes;

        // Deserialization constructor
        public HedgingPolicy() {}

        /**
         * Constructor.
         *
         * @param maxAttempts max number of hedging attempts, including the initial attempt
         * @param hedgingDelay duration delay between attempts
         * @param nonFatalStatusCodes list of returned status codes that will not cancel the request
         */
        public HedgingPolicy(
            Double maxAttempts, String hedgingDelay, List<String> nonFatalStatusCodes) {
          this.maxAttempts = maxAttempts;
          this.hedgingDelay = hedgingDelay;
          this.nonFatalStatusCodes = nonFatalStatusCodes;
        }

        public Double getMaxAttempts() {
          return maxAttempts;
        }

        public String getHedgingDelay() {
          return hedgingDelay;
        }

        public List<String> getNonFatalStatusCodes() {
          return nonFatalStatusCodes;
        }
      }
    }

    /** Method name identifier. */
    @JsonInclude(Include.NON_NULL)
    public static class MethodName {
      private String service;
      private String method;

      // Deserialization constructor
      public MethodName() {}

      /**
       * Constructor.
       *
       * @param service service name
       * @param method method name
       */
      public MethodName(String service, String method) {
        this.service = service;
        this.method = method;
      }

      public String getService() {
        return service;
      }

      public String getMethod() {
        return method;
      }
    }

    /**
     * Retry throttling configuration. See <a
     * href="https://github.com/grpc/proposal/blob/master/A6-client-retries.md#throttling-retry-attempts-and-hedged-rpcs">docs</a>.
     */
    @JsonInclude(Include.NON_NULL)
    public static class RetryThrottlingConfig {
      private Double maxTokens;
      private Double tokenRatio;

      // Deserialization constructor
      public RetryThrottlingConfig() {}

      /**
       * Constructor.
       *
       * @param maxTokens tokens per backend.
       * @param tokenRatio ratio for refilling tokens
       */
      public RetryThrottlingConfig(Double maxTokens, Double tokenRatio) {
        this.maxTokens = maxTokens;
        this.tokenRatio = tokenRatio;
      }

      public Double getMaxTokens() {
        return maxTokens;
      }

      public Double getTokenRatio() {
        return tokenRatio;
      }
    }
  }
}
