package com.mikasaco.syncdata.model;

import java.io.Serializable;
import java.util.List;

/**
 * 需要分页的请求 封装类
 *
 * @author wt 2018/7/30 10:57 AM
 */
public class Page<T> implements Serializable {

    private int pageSize = 10;

    private int pageNo = 1;

    private List<T> data;

    private int total = 0;

    private int offset = 0;

    public Page() {
    }

    public Page(int pageSize, int pageNo) {
        this.pageSize = pageSize;
        this.pageNo = pageNo;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public int getOffset() {
        return pageSize * (pageNo - 1);
    }

    public void setOffset(int pagesize, int pageNo) {
        this.offset = (pageNo-1) * pagesize > 0 ? (pageNo-1) * pagesize : 0 ;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
    }

    public int getTotal() {
        return total;
    }

    public void setTotal(int total) {
        this.total = total;
    }
}
