package com.kugou.test;

import com.kugou.loader.clickhouse.config.ConfigurationOptions;
import org.apache.commons.lang.StringUtils;
import org.junit.*;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by jaykelin on 2017/3/14.
 */

public class TextLoaderMapperTest {

    @org.junit.Test
    public void readLineTest(){
        char separator = '|';
        String nullNonString = "";
        String replaceChar = " ";

        StringBuilder line = new StringBuilder();
        int index = 0;
        int start = 0;
        int end   = 0;
        String raw = "xxx|网络汇总|版本汇总|搜索|关键字搜索|0|6418521|20317388|100|xxx|\\\\\\N|2017-03-13|";
        for(int i = 0; i < raw.length(); i++){
            if(raw.charAt(i) == separator){
                // 截取字段内容
                String field = raw.substring(start, i);
                end = i;
                start = end+1;

                if (getExcludeFieldIndexs().contains(index++)){
                    continue;
                }

                if(index > 1 && line.length() > 0){
                    line.append('\t');
                }
                if(field.equals("\\\\N") || field.equals("/N")){
                    field = StringUtils.isBlank(nullNonString)?"":nullNonString;
                }else{
                    field = field.replace('\t', replaceChar.charAt(0));
                    field = field.replace('\\', '/');
                }
                line.append(field);
                if(index == 10){
                    System.out.println();
                }
            }

        }
        if(start <= raw.length() && !getExcludeFieldIndexs().contains(index)){
            String field = raw.substring(start);

            line.append('\t').append(field);
        }


        System.out.println(line.toString());
    }

    protected Set getExcludeFieldIndexs(){
        Set set = new HashSet();
        set.add(0);set.add(10);
        return set;
    }
}
