package com.michalklempa.flink.exercise.datastream;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class ClickEvent {
    public long client_time;
    public String page;
    public long user_id;

    public ClickEvent() {
    }

    public ClickEvent(long client_time, String page, long user_id) {
        this.client_time = client_time;
        this.page = page;
        this.user_id = user_id;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
                .append("client_time", client_time)
                .append("page", page)
                .append("user_id", user_id)
                .toString();
    }
}
