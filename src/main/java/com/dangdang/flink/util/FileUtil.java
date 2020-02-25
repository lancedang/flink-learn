// Copyright (C) 2020 Meituan
// All rights reserved
package com.dangdang.flink.util;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * @author qiankai07
 * @version 1.0
 * Created on 2/25/20 11:34 PM
 **/
public class FileUtil {
    public static String readSourceFile(String rs) throws Exception {
        Class<FileUtil> testHelperClass = FileUtil.class;
        ClassLoader classLoader = testHelperClass.getClassLoader();
        URI uri = classLoader.getResource(rs).toURI();
        byte[] bytes = Files.readAllBytes(Paths.get(uri));
        return new String(bytes, "UTF-8");
    }
}