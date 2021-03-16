package com.github.vincentrussell.query.mongodb.sql.converter;

import org.apache.commons.cli.*;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Scanner;

/**
 * Created by tingfeng on 2018/10/22.
 */
public class MainDevTest {
    //java -jar target/xxx.jar -i -h localhost:3086 -db local -b 5

    public static void main(String[] args) throws ClassNotFoundException, org.apache.commons.cli.ParseException, ParseException, IOException {
        String commandLine = "-sql#"+"select a,b,c,d.* from my_table where (a>1 or b>2) and c>3 order by abc";
//        String commandLine = "-sql#"+"select * from my_table where date(column,'natural') >= '5000 days ago'";
        Main.main(StringUtils.split(commandLine,"#"));
    }
}
