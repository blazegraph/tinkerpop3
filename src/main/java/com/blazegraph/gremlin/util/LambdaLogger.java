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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Wrap conditional logging using Java 8 lambdas.
 * 
 * To use this functionality, simply request a LambdaLogger instead of a normal Logger:
 * 
 * LambdaLogger log = LambdaLogger.getLogger(Foo.class)
 * 
 * To log conditionally, simply use this syntax:
 * 
 * log.debug(() -> getMessage()); // getMessage() only called if log level >= DEBUG
 * 
 * In this example getMessage() will not be called unless the log level is
 * set to DEBUG or higher.  Conditional logging drastically improves
 * performance and should ALWAYS be preferred in production (non-test) code 
 * over the unconditional logging syntax:
 * 
 * log.debug(getMessage()); // getMessage() called regardless of log level
 * 
 * @author mikepersonick
 */
public class LambdaLogger extends Logger {

    //    private static String FQCN = Logger.class.getName();
    private static String FQCN = LambdaLogger.class.getName();
    
    /**
     * Run the supplied code, quieting the supplied log at Level.ERROR
     */
    public static final void quiet(Class<?> cls, Code code) throws Exception {
        quiet(cls, Level.OFF, code);
    }
    
    /**
     * Run the supplied code, quieting the supplied log with the specified Level
     */
    public static final void quiet(Class<?> cls, Level set, Code code) throws Exception {
        
        final Logger log = Logger.getLogger(cls);
        final Level curr = log.getLevel();
        log.setLevel(set);
        try {
            code.run();
        } finally {
            log.setLevel(curr);
        }
        
    }
    
    private static final Map<String,LambdaLogger> loggers = new ConcurrentHashMap<>();
    
    /**
     * Override to return a LambdaLogger.
     */
    public static LambdaLogger getLogger(final String name) {
        if (loggers.containsKey(name)) {
            return loggers.get(name);
        } else {
            final LambdaLogger log = new LambdaLogger(Logger.getLogger(name));
            final LambdaLogger curr = loggers.putIfAbsent(name, log);
            return curr != null ? curr : log;
        }
    }

    /**
     * Override to return a LambdaLogger.
     */
    @SuppressWarnings("rawtypes") 
    public static LambdaLogger getLogger(final Class c) {
        return LambdaLogger.getLogger(c.getName());
    }

    private final Logger log;
    
    private LambdaLogger(final Logger log) {
        super(log.getName());
        this.log = log;
        this.repository = log.getLoggerRepository();
        this.parent = log.getParent();
    }
    
    /**
     * Checked exception throwing version of Supplier<T>.
     * 
     * @author mikepersonick
     */
    @FunctionalInterface
    public interface Supplier<T> {
        public T get() throws Exception ;
    }
    
    /**
     * Conditionally log at the specified level. Does not call supplier.get() 
     * unless log.getLevel() >= level (conditional message production).
     * 
     * If the supplied object is a stream, conditionally log each object in
     * the stream on a separate line.
     */
    protected void log(final Level level, final Supplier<Object> supplier) {
        if (log.getLoggerRepository().isDisabled(level.toInt())) {
            return;
        }
        if (level.isGreaterOrEqual(log.getEffectiveLevel())) {
            final Object o = Code.wrapThrow(() -> supplier.get());
            if (o instanceof Stream) {
                final Stream<?> stream = (Stream<?>) o;
                try {
                    stream.forEach(msg -> {
                        log.callAppenders(new LambdaLoggingEvent(level, msg));
                    });
                } finally {
                    stream.close();
                }
            } else {
                log.callAppenders(new LambdaLoggingEvent(level, o));
            }
        }
    }
    
    /**
     * Conditionally log at INFO. Does not call supplier.get() unless
     * log.isInfoEnabled() == true (conditional message production).
     * 
     * If the supplied object is a stream, conditionally log each object in
     * the stream on a separate line.
     */
    public void info(final Supplier<Object> supplier) {
        log(Level.INFO, supplier);
    }
    
    /**
     * Conditionally log at DEBUG.  Does not call supplier.get() unless
     * log.isDebugEnabled() == true (conditional message production).
     * 
     * If the supplied object is a stream, conditionally log each object in
     * the stream on a separate line.
     */
    public void debug(final Supplier<Object> supplier) {
        log(Level.DEBUG, supplier);
    }
    
    /**
     * Conditionally log at TRACE. Does not call supplier.get() unless
     * log.isTraceEnabled() == true (conditional message production).
     * 
     * If the supplied object is a stream, conditionally log each object in
     * the stream on a separate line.
     */
    public void trace(final Supplier<Object> supplier) {
        log(Level.TRACE, supplier);
    }
    
    /**
     * Had to override this to parse the stack trace differently when dealing
     * with lambdas to get the correct class / method / line number.
     * 
     * @author mikepersonick
     */
    private class LambdaLoggingEvent extends LoggingEvent {
        private static final long serialVersionUID = 8785675398139952600L;
        
        public LambdaLoggingEvent(Priority level, Object message) {
            super(FQCN, LambdaLogger.this, level, message, null);
        }
        
        private LocationInfo location = null;
        public LocationInfo getLocationInformation() {
            if (location == null) {
                Throwable t = new Throwable();
//                t.printStackTrace();
                location = new LambdaLocation(t);
            }
            return location;
        }
        
    }
    
    /**
     * Had to override this to parse the stack trace differently when dealing
     * with lambdas to get the correct class / method / line number.
     * 
     * @author mikepersonick
     */
    private class LambdaLocation extends LocationInfo {
        private static final long serialVersionUID = -7711773641304797646L;

        @Override
        public String getClassName() {
            return className;
        }

        @Override
        public String getFileName() {
            return fileName;
        }

        @Override
        public String getLineNumber() {
            return lineNumber;
        }

        @Override
        public String getMethodName() {
            return methodName;
        }

        private final String className;
        private final String methodName;
        private final String fileName;
        private final String lineNumber;

        public LambdaLocation(Throwable t) {
            super(t, FQCN);

            String className = null;
            String methodName = null;
            String fileName = null;
            String lineNumber = null;
            
            /*
             * Go top-down instead of bottom up like the default impl.
             */
            StackTraceElement[] elements = t.getStackTrace();
            for (int i = 0; i < elements.length; i++) {
                String thisClass = elements[i].getClassName();
                String thisMethod = elements[i].getMethodName();
                if (FQCN.equals(thisClass) && "log".equals(thisMethod)) {
                    int caller = i + 2;
                    if (caller < elements.length) {
                        className = elements[caller].getClassName();
                        methodName = elements[caller].getMethodName();
                        fileName = elements[caller].getFileName();
                        int line = elements[caller].getLineNumber();
                        lineNumber = line < 0 ? NA : String.valueOf(line);
                        StringBuilder buf = new StringBuilder();
                        buf.append(className);
                        buf.append(".");
                        buf.append(methodName);
                        buf.append("(");
                        buf.append(fileName);
                        buf.append(":");
                        buf.append(lineNumber);
                        buf.append(")");
                        this.fullInfo = buf.toString();
                    }
                    break;
                }
            }
            
            this.className = className != null ? className : NA;
            this.methodName = methodName != null ? methodName : NA;
            this.fileName = fileName != null ? fileName : NA;
            this.lineNumber = lineNumber != null ? lineNumber : NA;
        }
        
    }
    
}
