package com.xyw55;

import org.json.simple.JSONObject;

/**
 * Created by xiayiwei on 16/4/24.
 */
public class Risk {
    private String type;
    private String content;
    private String pcap_id;
    private float score;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getPcap_id() {
        return pcap_id;
    }

    public void setPcap_id(String pcap_id) {
        this.pcap_id = pcap_id;
    }

    public float getScore() {
        return score;
    }

    public void setScore(float score) {
        this.score = score;
    }


    public JSONObject toJSONObject(){
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("type", type);
        jsonObject.put("content", content);
        jsonObject.put("pcap_id", pcap_id);
        jsonObject.put("score", score);
        return jsonObject;
    }
}
