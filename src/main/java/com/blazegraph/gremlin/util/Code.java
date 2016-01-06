/**
Copyright (C) SYSTAP, LLC 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.blazegraph.gremlin.util;

import java.util.concurrent.Callable;

/**
 * Override of Runnable to throw exceptions instead of trapping.  Useful for
 * lambdas.
 * 
 * @author mikepersonick
 */
@FunctionalInterface
public interface Code {

    /**
     * Execute the code block without trapping exceptions.
     *
     * @throws Exception
     */
    public abstract void run() throws Exception;
    
    /**
     * Execute the callable wrapping checked exceptions inside a 
     * RuntimeException.
     */
    public static <T> T wrapThrow(Callable<T> callable) {
        try {
            return callable.call();
        } catch (RuntimeException e) {
//            e.printStackTrace();
            throw e;
        } catch (Exception e) {
//            e.printStackTrace();
            throw new RuntimeException(e);
        }
    } 
    
    /**
     * Execute the code block wrapping checked exceptions inside a 
     * RuntimeException.
     */
    public static void wrapThrow(Code code) {
        try {
            code.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    } 
    
    /**
     * Execute the callable wrapping checked exceptions inside a 
     * RuntimeException.
     */
    public static <T> T wrapThrow(Callable<T> callable, Code _finally) {
        try {
            return callable.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            wrapThrow(_finally);
        }
    } 
    
    /**
     * Execute the code block wrapping checked exceptions inside a 
     * RuntimeException.
     */
    public static void wrapThrow(Code code, Code _finally) {
        try {
            code.run();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            wrapThrow(_finally);
        }
    } 
    
    /**
     * Execute the callable without trapping checked exceptions. Use instead
     * of a wrap/re-throw pattern inside lambdas.
     */
    public static <T> T unchecked(Callable<T> callable) {
        try {
            return callable.call();
        } catch (Exception e) {
            return sneakyThrow(e);
        }
    }

    /**
     * Execute the code block without trapping checked exceptions. Use instead
     * of a wrap/re-throw pattern inside lambdas.
     */
    public static void unchecked(Code code) {
        try {
            code.run();
        } catch (Exception e) {
            sneakyThrow(e);
        }
    }

    /**
     * Execute the callable without trapping checked exceptions. Use instead
     * of a wrap/re-throw pattern inside lambdas.
     */
    public static <T> T unchecked(Callable<T> callable, Code _finally) {
        try {
            return callable.call();
        } catch (Exception e) {
            return sneakyThrow(e);
        } finally {
            unchecked(_finally);
        }
    }

    /**
     * Execute the code block without trapping checked exceptions. Use instead
     * of a wrap/re-throw pattern inside lambdas.
     */
    public static void unchecked(Code code, Code _finally) {
        try {
            code.run();
        } catch (Exception e) {
            sneakyThrow(e);
        } finally {
            unchecked(_finally);
        }
    }

    /**
     * Use type erasure to throw checked exceptions without declaring them,
     * bypassing compiler checks.  Use instead of a wrap/re-throw pattern
     * inside lambdas.   
     */
    public static <T> T sneakyThrow(Throwable e) {
        return Code.<RuntimeException, T> sneakyThrow0(e);
    }

    /**
     * Use type erasure to throw checked exceptions without declaring them,
     * bypassing compiler checks.  Use instead of a wrap/re-throw pattern
     * inside lambdas.   
     */
    @SuppressWarnings("unchecked")
    public static <E extends Throwable, T> T sneakyThrow0(Throwable t) throws E {
        throw (E) t;
    }

}
