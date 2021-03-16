package com.github.vincentrussell.query.mongodb.sql.converter;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;

/**
 * Created by tingfeng on 2018/10/22.
 */
public class Main1DevTest {
    //java -jar target/xxx.jar -i -h localhost:3086 -db local -b 5

    public static void main(String[] args) throws ClassNotFoundException, org.apache.commons.cli.ParseException, ParseException, IOException {
//        String commandLine = "-i -h 192.168.0.217:27017 -db dms_admin -a admin -u admin -p admin -b 5";
//        Main.main(StringUtils.split(commandLine," "));

        String commandLine = "-i -h 192.168.0.217:27017 -db dms_admin -a admin -u admin -p admin -b 5";
        Main1.main(StringUtils.split(commandLine, " "));
    }
}
