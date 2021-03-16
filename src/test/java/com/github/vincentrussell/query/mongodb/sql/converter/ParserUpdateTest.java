package com.github.vincentrussell.query.mongodb.sql.converter;

import com.google.common.base.Charsets;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.StreamProvider;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.update.Update;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

public class ParserUpdateTest {
    public static void main(String[] args) {
        System.out.println("Hello World!");

        String sql = "update table1 set name='yy',sex=2 where id =1";

        try {
            byte[] bytes = sql.getBytes(Charsets.UTF_8);
            InputStream inputStream = new ByteArrayInputStream(bytes);
            StreamProvider streamProvider = new StreamProvider(inputStream, Charsets.UTF_8.name());
            CCJSqlParser jSqlParser = new CCJSqlParser(streamProvider);
            Statement statement = jSqlParser.Statement();
            Update update =   (Update)statement;
            String tableName = update.getTables().get(0).getName();
            Expression where = update.getWhere();
            if(where instanceof EqualsTo){
                EqualsTo e = (EqualsTo) where;
                System.out.println(e.getLeftExpression().toString());
                System.out.println(e.getRightExpression().toString());
            }
            List<Column> columns = update.getColumns();
            List<Expression> expressions = update.getExpressions();
            System.out.println(statement);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}