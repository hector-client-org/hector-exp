package com.netflix.astyanax.connectionpool.exceptions;

import com.netflix.astyanax.connectionpool.exceptions.*;
import com.netflix.astyanax.connectionpool.exceptions.AuthenticationException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import org.apache.cassandra.thrift.*;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.transport.TTransportException;

import java.net.SocketTimeoutException;

/**
 * @author zznate
 */
public class ThriftExceptionConverter {
  /**
   * Convert from Thrift exceptions to an internal ConnectionPoolException
   *
   * @param e
   * @return
   */
  public static ConnectionException ToConnectionPoolException(Exception e) {
      if (e instanceof ConnectionException) {
          return (ConnectionException) e;
      }

      if (e instanceof InvalidRequestException) {
          return new com.netflix.astyanax.connectionpool.exceptions.BadRequestException(
                  e);
      } else if (e instanceof TProtocolException) {
          return new com.netflix.astyanax.connectionpool.exceptions.BadRequestException(
                  e);
      } else if (e instanceof UnavailableException) {
          return new TokenRangeOfflineException(e);
      } else if (e instanceof SocketTimeoutException) {
          return new TimeoutException(e);
      } else if (e instanceof TimedOutException) {
          return new OperationTimeoutException(e);
      } else if (e instanceof NotFoundException) {
          return new com.netflix.astyanax.connectionpool.exceptions.NotFoundException(
                  e);
      } else if (e instanceof TApplicationException) {
          return new ThriftStateException(e);
      } else if (e instanceof AuthenticationException
              || e instanceof AuthorizationException) {
          return new com.netflix.astyanax.connectionpool.exceptions.AuthenticationException(
                  e);
      } else if (e instanceof TTransportException) {
          if (e.getCause() != null) {
              if (e.getCause() instanceof SocketTimeoutException) {
                  return new TimeoutException(e);
              }
              if (e.getCause().getMessage() != null) {
                  if (e.getCause().getMessage().toLowerCase()
                          .contains("connection abort")
                          || e.getCause().getMessage().toLowerCase()
                                  .contains("connection reset")) {
                      return new ConnectionAbortedException(e);
                  }
              }
          }
          return new TransportException(e);
      } else {
          // e.getCause().printStackTrace();
          return new UnknownException(e);
      }
  }
}
