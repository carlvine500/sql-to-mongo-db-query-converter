package com.github.vincentrussell.query.mongodb.sql.converter;

import com.alibaba.fastjson.JSON;
import com.google.common.base.Charsets;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.StreamProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.util.deparser.SelectDeParser;
import org.apache.commons.lang.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.*;

public class ParserInsertTest {
    public static void main(String[] args) {
        System.out.println("Hello World!");

        String sql = "insert into table1(name,age) values('xx',1)";
//        String sql = "update table1 set name='yy' where id =1";

        try {
            byte[] bytes = sql.getBytes(Charsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(bytes);
            StreamProvider streamProvider = new StreamProvider(inputStream, Charsets.UTF_8.name());
            CCJSqlParser jSqlParser = new CCJSqlParser(streamProvider);
            Statement statement = jSqlParser.Statement();
            Insert insert = (Insert)statement;
            List<Column> columns = insert.getColumns();
            List<String> columnNames = new ArrayList<>();
            for (Column column : columns) {
                String columnName = column.getColumnName();
                if(StringUtils.contains(columnName,".")){
                    columnName = StringUtils.substringAfterLast(columnName, ".");
                }
                columnNames.add(columnName);
            }
            List<String> values = new ArrayList<String>();
            ItemsList itemsList = insert.getItemsList();
            List<Expression> expressions = ((ExpressionList)itemsList).getExpressions();
            for (Expression expression : expressions) {
                values.add(expression.toString());
            }
            System.out.println(columnNames);
            System.out.println(values);
            System.out.println(insert.getTable().getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}