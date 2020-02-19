// Copyright (C) 2020 Meituan
// All rights reserved
package com.dangdang.flink.ml;

/**
 * @author qiankai07
 * @version 1.0
 * Created on 2/18/20 4:29 PM
 **/
public class CsvData {
    private int x;
    private int y;

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }

    @Override
    public String toString() {
        return "csv data{x" + "=" + x + ",y=" + y + "}";
    }
}