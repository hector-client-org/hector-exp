package com.netflix.astyanax.connectionpool.impl;

import com.netflix.astyanax.connectionpool.Execution;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.IsRetryableException;
import com.netflix.astyanax.connectionpool.retry.RetryPolicy;

public abstract class AbstractExecutionImpl<R> implements Execution<R> {
    public OperationResult<R> executeWithRetry(RetryPolicy retry)
            throws ConnectionException {
        ConnectionException lastException = null;
        retry.begin();
        do {
            try {
                return execute();
            } catch (ConnectionException ex) {
                if (ex instanceof IsRetryableException)
                    lastException = ex;
                else
                    throw ex;
            }
        } while (retry.allowRetry());

        throw lastException;
    }
}
