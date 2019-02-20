package com.google.enterprise.cloudsearch.sdk;

import com.google.api.client.googleapis.GoogleUtils;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.util.Base64;
import com.google.enterprise.cloudsearch.sdk.config.Configuration;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Encapsulates an @{link Proxy} object and the token for proxy authentication. Following keys are
 * used by the proxy
 *
 * <ul>
 * <li>transport.proxy.type: HTTP or SOCKS.
 * <li>transport.proxy.hostname: host name of the proxy.
 * <li>transport.proxy.port: port of the proxy.
 * <li>transport.proxy.username: optional, username used to authenticate the proxy.
 * <li>transport.proxy.password: optional, password used to authenticate the proxy.
 * </ul>
 */
public final class GoogleProxy {

  private final Proxy proxy;
  private final Optional<String> authToken;

  /** configuration key for proxy type */
  public static final String TRANSPORT_PROXY_TYPE_KEY = "transport.proxy.type";
  /** configuration key for proxy hostname */
  public static final String TRANSPORT_PROXY_HOSTNAME_KEY = "transport.proxy.hostname";
  /** configuration key for proxy port */
  public static final String TRANSPORT_PROXY_PORT_KEY = "transport.proxy.port";
  /** configuration key for proxy username */
  public static final String TRANSPORT_PROXY_USERNAME_KEY = "transport.proxy.username";
  /** configuration key for proxy password */
  public static final String TRANSPORT_PROXY_PASSWORD_KEY = "transport.proxy.password";

  /** Creates an {@link GoogleProxy} instance based on proxy configuration. */
  public static GoogleProxy fromConfiguration() {
    checkState(Configuration.isInitialized(), "configuration must be initialized");

    String userName = Configuration.getString(TRANSPORT_PROXY_USERNAME_KEY, "").get();
    String password = Configuration.getString(TRANSPORT_PROXY_PASSWORD_KEY, "").get();
    GoogleProxy.Builder builder = new GoogleProxy.Builder();
    if (!userName.isEmpty() && !password.isEmpty()) {
      builder.setUserNamePassword(userName, password);
    }

    Proxy.Type proxyType =
        Configuration.getValue(
            TRANSPORT_PROXY_TYPE_KEY,
            Proxy.Type.HTTP,
            value -> {
              try {
                return Proxy.Type.valueOf(value);
              } catch (IllegalArgumentException e) {
                throw new InvalidConfigurationException(e);
              }
            })
            .get();
    String hostname = Configuration.getString(TRANSPORT_PROXY_HOSTNAME_KEY, "").get();
    int port = Configuration.getInteger(TRANSPORT_PROXY_PORT_KEY, -1).get();

    if (!hostname.isEmpty()) {
      Configuration.checkConfiguration(port > 0, String.format("port %d is invalid", port));
    }

    Proxy proxy =
        hostname.isEmpty()
            ? Proxy.NO_PROXY
            : new Proxy(proxyType, new InetSocketAddress(hostname, port));

    return builder.setProxy(proxy).build();
  }

  private GoogleProxy(Proxy proxy, Optional<String> authToken) {
    this.proxy = proxy;
    this.authToken = authToken;
  }

  public Proxy getProxy() {
    return proxy;
  }

  /** Gets an {@link HttpTransport} that contains the proxy configuration. */
  public HttpTransport getHttpTransport() throws IOException, GeneralSecurityException {
    return new NetHttpTransport.Builder()
        .trustCertificates(GoogleUtils.getCertificateTrustStore())
        .setProxy(proxy)
        .build();
  }

  /** Gets an {@link HttpRequestInitializer} that sets the http header for proxy authorization. */
  public HttpRequestInitializer getHttpRequestInitializer() {
    return authToken.isPresent()
        ? httpRequest ->
            httpRequest
                .getHeaders()
                .set("Proxy-Authorization", String.format("Basic %s", authToken.get()))
        : httpRequest -> {};
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof GoogleProxy)) {
      return false;
    }
    GoogleProxy googleProxy = (GoogleProxy) other;
    return Objects.equals(proxy, googleProxy.proxy)
        && Objects.equals(authToken, googleProxy.authToken);
  }

  @Override
  public int hashCode() {
    return Objects.hash(proxy, authToken);
  }

  /** Builder for {@link GoogleProxy}. */
  public static class Builder {

    private Proxy proxy = Proxy.NO_PROXY;
    private String authToken;

    /** Specifies a provided {@link Proxy} instance. */
    public Builder setProxy(Proxy proxy) {
      this.proxy = proxy;
      return this;
    }

    /** Optional. Specifies a user name and password for proxy authentication. */
    public Builder setUserNamePassword(String userName, String password) {
      this.authToken =
          Base64.encodeBase64String(String.format("%s:%s", userName, password).getBytes());
      return this;
    }

    /** Builds a {@link GoogleProxy} instance. */
    public GoogleProxy build() {
      checkNotNull(proxy);
      return new GoogleProxy(proxy, Optional.ofNullable(authToken));
    }
  }
}
