package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.Charsets;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.StreamProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.update.Update;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public class ParserDeleteTest {
    public static void main(String[] args) {
        System.out.println("Hello World!");

        String sql = "delete from table1 where id =1";

        try {
            byte[] bytes = sql.getBytes(Charsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(bytes);
            StreamProvider streamProvider = new StreamProvider(inputStream, Charsets.UTF_8.name());
            CCJSqlParser jSqlParser = new CCJSqlParser(streamProvider);
            Statement statement = jSqlParser.Statement();
            Delete delete = (Delete) statement;
            Table ts = delete.getTable();
            Expression where = delete.getWhere();
            if(where instanceof EqualsTo){
                EqualsTo e = (EqualsTo) where;
                System.out.println(e.getLeftExpression().toString());
                System.out.println(e.getRightExpression().toString());
            }
            System.out.println(ts.getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}