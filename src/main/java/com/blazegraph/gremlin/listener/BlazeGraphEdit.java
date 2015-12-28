package com.blazegraph.gremlin.listener;

public class BlazeGraphEdit { 
    
    public static enum Action {
        
        Add, Remove;
        
    }

    private final Action action;

    private final BlazeGraphAtom atom;
    
    private final long timestamp;
    
    public BlazeGraphEdit(final Action action, final BlazeGraphAtom atom) {
        this(action, atom, 0l);
    }

    public BlazeGraphEdit(final Action action, final BlazeGraphAtom atom, 
            final long timestamp) {
        this.action = action;
        this.atom = atom;
        this.timestamp = timestamp;
    }

    public Action getAction() {
        return action;
    }

    public BlazeGraphAtom getAtom() {
        return atom;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public String toString() {
        return "BigdataGraphEdit [action=" + action + ", atom=" + atom
                + ", timestamp=" + timestamp + "]";
    }

}

