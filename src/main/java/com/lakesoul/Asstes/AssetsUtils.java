package com.lakesoul.Asstes;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AssetsUtils {

    public String[] parseFileOpsString(String fileOPs) {

        String[] fileInfo = new String[2];
        Pattern pattern = Pattern.compile("\\(([^,]+),([^,]+),([^,]+),\"([^\"]+)\"\\)");

        Matcher matcher = pattern.matcher(fileOPs);
        if (matcher.find()) {
            // 提取匹配的各个部分
            String actionType = matcher.group(2);
            String fileSize = matcher.group(3);
            fileInfo[0] = actionType;
            fileInfo[1] = fileSize;
        }

        return fileInfo;
    }

}