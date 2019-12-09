package com.mikasaco.syncdata.entity;

import lombok.Data;

import java.util.HashMap;
import java.util.List;

@Data
public class CanalMessage {

    private String database;

    private String type;

    private Boolean isDdl;

    private Integer id;

    private String data;

    private Long es;

    private String table;

    private List<HashMap<String, String>> old;

}
