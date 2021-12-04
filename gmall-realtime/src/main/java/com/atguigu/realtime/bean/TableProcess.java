package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableProcess {
    private String source_table ;
    private String operate_type ;
    private String sink_type ;
    private String sink_table ;
    private String sink_columns ;
    private String sink_pk ;
    private String sink_extend ;
};
