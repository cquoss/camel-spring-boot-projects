package de.quoss.camel.spring.boot.jms.xa.exception;

public class JmsXaException extends RuntimeException {

    public static final long serialVersionUID = 1L;

    public JmsXaException(final String s) {
        super(s);
    }

    public JmsXaException(final Throwable t) {
        super(t);
    }

    public JmsXaException(final String s, final Throwable t) {
        super(s, t);
    }

}
