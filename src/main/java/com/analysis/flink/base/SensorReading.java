package com.analysis.flink.base;

/**
 * @author: tianyong
 * @time: 2021/9/14 9:19
 * @description:
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 */
public class SensorReading {

    private String id;
    private Long time;
    private Double temperature;

    public SensorReading(String id, Long time, Double temperature) {
        this.id = id;
        this.time = time;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public Double getTemperature() {
        return temperature;
    }

    public void setTemperature(Double temperature) {
        this.temperature = temperature;
    }
}
