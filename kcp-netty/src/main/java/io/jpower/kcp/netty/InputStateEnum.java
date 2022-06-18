package io.jpower.kcp.netty;

public enum InputStateEnum {

    NORMAL(0, "normal"),
    NO_ENOUGH_HEAD(-1, "No enough bytes of head"),
    NO_ENOUGH_DATA(-2, "No enough bytes of data"),
    MISMATCH_CMD(-3, "Mismatch cmd"),
    INCONSISTENCY_CONV(-4, "Conv inconsistency"),
    ;

    InputStateEnum(int state, String description) {
        this.state = state;
        this.description = description;
    }

    private int state;
    private String description;

}
