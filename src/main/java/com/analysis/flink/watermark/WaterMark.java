package com.analysis.flink.watermark;

/**
 * @author: tianyong
 * @time: 2021/9/14 14:04
 * @description: watermark
 * @Version: v1.0
 * @company: Qi An Xin Group.Situation 态势感知事业部
 * @desc watermark：是触发前一窗口的关闭时间，
 *                  是一种延迟机制，相当于表调慢了
 */
public class WaterMark {

    // MyAssigner 有两种类型
    // 	AssignerWithPeriodicWatermarks (针对于有序数据)
    //      周期性将watermark插入到流中
    // 	AssignerWithPunctuatedWatermarks (针对于无序数据)
    //      间断性将watermark插入到流中

}
